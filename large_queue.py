# large_queue.py

"""
    LargeQueue
    
    The implementation provides a rough solution for certain issues when implementing a FIFO queue or a circular buffer
    in Aerospike.
    
    1. Large queue size. The List data type in Aerospike can be easily leveraged to implement a queue. However
        the maximum Aerospike record size is 8MB which is the outer limit of the queue size thus implemented. 
        LargeQueue allows an arbitrarily sized queue by using multiple records. 
    2. Consistency across multiple records. In Aerospike, single record operations are atomic. Queue operations are 
        performed under a queue lock to preserve consistency across multiple records.
            - Slow clients and client failures. A new lock request will break the old lock if it is timed out. 
            - Operations whose lock is broken. Updates are prevented from committing if their lock is broken by
                checking if the lock is still held before committing new head/tail pointers in the metadata record.
            - Rollback of failed updates (broken locks). A rollback of an update is not necessary as a commit happens
                in a final atomic step on a single metadata record where the head/tail pointers are updated.
    3. Fencing to guard against lingering invalid writes. An enqueue operation whose lock is broken is prevented from 
                overwriting a subsequent valid enqueue entry by use of a fencing marker in the record. Only writes
                with a higher fencing token than the last fencing marker in the record are allowed.
    4. Efficient operations. With a circular buffer implementation, head/tail pointers can be advanced monotonically
        without costly copy and space management.
            - Offset based access to entries. A monotonically increasing offset indicates the position of an entry 
                in the queue, and an entry can be read using a valid offset without acquiring a lock.
    
    Design
    
    The FIFO queue is implemented as a circular buffer using a "metadata" record that holds queue metadata such as the 
    lock info and head/tail positions and several "buffer" records that hold the entries in the queue. 
    
    The head  points to the first entry and the tail points to position AFTER the last entry. An empty and full queue 
    point to the same physical position in the circular buffer, the difference being that the head and tail offsets 
    are different for a full queue by the circular buffer size. 
    
    An enqueue and dequeue operation acquire the lock in the metadata record, read or write the head or tail entry,
    and in the last step atomically release the lock and update head/tail pointer. A lock that has expired is 
    broken by a new lock request and the lock is granted to the requester. The old requester cannot commit as 
    the commit step checks that the lock is still held by the requester; if not, the request is aborted. Similarly,
    the old requester is prevented from overwriting a valid entry with a fencing scheme. Each enqueue request gets a 
    monotonically increasing fencing token, and updates the buffer record with fencing token as it appends
    the new entry. An older enqueue operation with a smaller fencing token than in the record is disallowed.
    
    The enqueue operation allows the circular buffer to be overwritten when the queue is full with the 
    parameter "overwrite_if_full" parameter. By default, this parameter is False and an exception is raised. 
    
    A monotonically increasing queue "offset" is maintained and an entry can be read with a valid offset (that is,
    between head and tail positions) with "get_entry_by_offset". This function requires no lock and offers no 
    guarantee that an entry exists or has not been removed at that offset.
    
    The "transaction-id" required to assign the lock needs to be unique across all concurrent requests. It need not 
    be unique across operations that are not concurrent. 
 
    Potential future enhancements
    
    - Allow many consumer groups each with its own tracking metadata.
    - Separate namepaces to store metatdata and buffer records. Performance can benefit by storing the metadata 
        namspace in faster persistent storage (like PMEM).
     - Extend metadata record to hold recent committed transactions for the ability to ascertain status by 
        transaction-id when various failures make an operation outcome unknown and the operation cannot  
        be simply resubmitted.
"""

from __future__ import print_function
import time
import aerospike
from aerospike import predexp as predexp
from aerospike import exception as exception
from aerospike_helpers.operations import operations as op_helpers
from aerospike_helpers.operations import list_operations as list_helpers


class ASAborted(Exception):
    def __init__(self, reason):
        self.reason = reason


class LargeQueue(object):
    META_REC_KEY = 'queue-metadata'
    BUF_REC_KEY_PREFIX = 'queue-buffer-'
    LOCK_MAX_RETRIES = 3
    LOCK_POLL_WAIT_MS = 100
    LOCK_EXPIRATION_MS = 200

    # Enum for queue operations implemented as class variables
    class Ops:
        Dequeue = 1
        Enqueue = 2

    @staticmethod
    def _buf_record_key(rec_index):
       return LargeQueue.BUF_REC_KEY_PREFIX + str(rec_index)

    @staticmethod
    def _curr_time_milliseconds():
        return int(time.time() * 1000)

    def __init__(self):
        """
        The null constructor.
        """
        self.client = None
        self.namespace = None
        self.name = None
        self.slots_per_rec = None
        self.num_buf_recs = None
        self.initialized = False

    @staticmethod
    def _get_metadata(client, namespace, q_name):
        """
        Get the metadata record.
        :param client: client object returned by aerospike.connect()
        :param namespace: namespace where the queue records are stored
        :param q_name: name of the queue, used as the "set" name
        :return: metadata record if queue exists, otherwise None
        """
        metadata_key = (namespace, q_name, LargeQueue.META_REC_KEY)
        try:
            (key, meta, record) = client. get(metadata_key)
        except exception.RecordNotFound as ex:
            return None
        return record

    def get_queue_info(self):
        """
        Get queue info.
        :return: a dict with externally visible attributes of the queue
        """
        if not self.initialized:
            return None
        record = LargeQueue._get_metadata(self.client, self.namespace, self.name)
        return { 'name': self.name,
                 'max-size': self.num_buf_recs * self.slots_per_rec,
                 'namespace': self.namespace,
                 'head-offset': long(record['head-offset']),
                 'tail-offset': long(record['tail-offset']) }

    def _create_metadata_record(self):
        """
        Create a metadata record for a new queue.
        :throws: ASAborted('Queue already exists')
        """
        # create the metadata record
        write_policy = { 'exists': aerospike.POLICY_EXISTS_CREATE,
                         'key': aerospike.POLICY_KEY_SEND }
        metadata_key = (self.namespace, self.name, LargeQueue.META_REC_KEY)
        metadata_bins = { 'locked': 0,
                          'lock-owner': None,
                          'lock-time-ms': None,
                          'head-offset': 0,
                          'tail-offset': 0,
                          'fencing-ctr': 0,
                          'num-buf-recs': self.num_buf_recs,
                          'slots-per-rec': self.slots_per_rec }
        try:
            self.client. put(metadata_key, metadata_bins, write_policy)
        except exception.RecordExistsError as ex:
            raise ASAborted('Queue already exists')
        return

    def _create_buf_records(self):
        """
        Create buffer records for a new queue.
        """
        # insert buffer records
        write_policy = { 'exists': aerospike.POLICY_EXISTS_CREATE_OR_REPLACE,
                         'key': aerospike.POLICY_KEY_SEND }
        buf_bins = { 'fencing-mark': 0,
                     'entries': [] }
        for i in range(self.slots_per_rec):
            buf_bins['entries'].append({ 'offset': -1, 'value': None })
        for i in range(self.num_buf_recs):
            buf_key = (self.namespace, self.name, LargeQueue._buf_record_key(i))
            _ = self.client. put(buf_key, buf_bins, write_policy)
        return

    def _reset_fencing_marks(self):
        """
        Reset the fencing marker in buffer and metadata records when the fencing counter in metadata record wraps
        around to a non-positive value. While like to be very infrequent, if at all necessary, operation
        (a long fencing counter should make it unnecessary), it is critical for it to succeed.
        If it fails for some reason, enqueue operations will fail due to fencing error until the fencing marker is
        reset.
        """
        write_policy = { 'exists': aerospike.POLICY_EXISTS_UPDATE }
        try:
            for i in range(self.num_buf_recs):
                buf_key = (self.namespace, self.name, LargeQueue._buf_record_key(i))
                self.client. put(buf_key, {'fencing_mark': 0}, write_policy)
            metadata_key = (self.namespace, self.name, LargeQueue.META_REC_KEY)
            self.client.put(metadata_key, {'fencing-ctr': 0}, write_policy)
        except ex:
            print('LargeQueue: critical error. Failure during reset of fencing marks', ex)
            raise ex
        return

    def create_new_queue(self, client, namespace, q_name, max_size, slots_per_rec):
        """
        Create a new queue using the input parameters.
        :param client: client object returned by aerospike.connect()
        :param namespace: namespace in which the queue records are to be stored
        :param q_name: name of the queue, used as the "set" name
        :param max_size: maximum number of entries to be held in the queue
        :param slots_per_rec: number of entries per record, depending on the size of entry. must be carefully
            selected otherwise record overflow can result at runtime.
        """
        self.client = client
        self.namespace = namespace
        self.name = q_name
        self.slots_per_rec = slots_per_rec
        self.num_buf_recs = (max_size + self.slots_per_rec - 1) / self.slots_per_rec
        self._create_metadata_record()
        self._create_buf_records()
        self.initialized = True
        return

    def initialize_existing_queue(self, client, namespace, q_name):
        """
        Initialize an existing queue in the given namespace with the given name.
        :param client: client object returned by aerospike.connect().
        :param namespace: namespace in which the queue is stored
        :param q_name: name of the queue
        """
        metadata = LargeQueue._get_metadata(client, namespace, q_name)
        if metadata is None:
            raise ASAborted('Queue does not exist')
        self.client = client
        self.namespace = namespace
        self.name = q_name
        self.slots_per_rec = metadata['slots-per-rec']
        self.num_buf_recs = metadata['num-buf-recs']
        self.initialized = True
        return

    def _lock(self, txn_id, op):
        """
        Atomically check if the queue is locked, break an expired lock, lock the queue and
        set the lock-owner and lock-time, and if the operation is enqueue, also increment and
        return the fencing counter.
        Try multiple times if the lock is not available, and wait before subsequent attempt.
        :param txn_id: lock owner id, must be unique among concurrent requests
        :param op: enqueue or dequeue
        :return: dict with head and tail positions on success
            throws ASAborted('Failed to acquire lock') on failure
        """
        metadata_key = (self.namespace, self.name, LargeQueue.META_REC_KEY)
        for _ in range(LargeQueue.LOCK_MAX_RETRIES):
            curr_time_ms = LargeQueue._curr_time_milliseconds()
            predexps = [ predexp.integer_bin("locked"),
                         predexp.integer_value(0),
                         predexp.integer_equal(),
                         predexp.integer_bin("lock-time-ms"),
                         predexp.integer_value(curr_time_ms-LargeQueue.LOCK_EXPIRATION_MS),
                         predexp.integer_less(),
                         predexp.predexp_or(2) ]
            ops = [ op_helpers.read('head-offset'),
                    op_helpers.read('tail-offset'),
                    op_helpers.write('locked', 1),
                    op_helpers.write('lock-owner', txn_id),
                    op_helpers.write('lock-time-ms', curr_time_ms) ]
            if op == LargeQueue.Ops.Enqueue:
                ops.append(op_helpers.increment('fencing-ctr', 1))
                ops.append(op_helpers.read('fencing-ctr'))
            try:
                _, _, record = self.client.operate(metadata_key, ops, policy={'predexp': predexps})
            except exception.FilteredOut as ex:     # predexp failed
                time.sleep(LargeQueue.LOCK_POLL_WAIT_MS/1000.0)
                continue
            return record
        raise ASAborted('Failed to acquire lock')

    def _commit_release(self, txn_id, new_head_offset=None, new_tail_offset=None):
        """
        If the lock is still held by this requester (txn-id), update the new positions of head/tail and r
        elease the lock. Otherwise abort the request as timed out.
        :param txn_id: lock owner id, must be unique among concurrent requests
        :param new_head_offset: new head offset to be updated
        :param new_tail_offset: new tail offset to be updated
        :return: throws ASAborted('Timed out')
        """
        metadata_key = (self.namespace, self.name, LargeQueue.META_REC_KEY)
        predexps = [ predexp.integer_bin("locked"),
                     predexp.integer_value(1),
                     predexp.integer_equal(),
                     predexp.integer_bin("lock-owner"),
                     predexp.integer_value(txn_id),
                     predexp.integer_equal(),
                     predexp.predexp_and(2)]
        ops = [ op_helpers.write('locked', 0),
                op_helpers.write('lock-owner', None),
                op_helpers.write('lock-time-ms', None) ]
        if new_head_offset is not None:
            ops.append(op_helpers.write('head-offset', new_head_offset))
        if new_tail_offset is not None:
            ops.append(op_helpers.write('tail-offset', new_tail_offset))
        try:
            _ = self.client.operate(metadata_key, ops, policy={'predexp': predexps})
        except exception.FilteredOut as ex:  # predexp failed
            raise ASAborted('Timed out')
        return

    def _get_entry_location(self, entry_offset):
        """
        Get the record index and entry index within the record, given the entry's offset.
        :param entry_offset: offset of the entry
        :return: tuple (record index, entry index)
        """
        buf_rec_index = int(entry_offset / self.slots_per_rec) % self.num_buf_recs
        entry_index = entry_offset % self.slots_per_rec
        return buf_rec_index, entry_index

    def _queue_is_full(self, head_offset, tail_offset):
        """
        Check if the queue is full.
        :param head_offset: Offset of the head entry.
        :param tail_offset: Offset of the tail entry (next added entry).
        :return: True if full, False otherwise
        """
        num_entries = tail_offset - head_offset
        return num_entries == self.num_buf_recs * self.slots_per_rec

    def _queue_is_empty(self, head_offset, tail_offset):
        """
        Check if the queue is empty.
        :param head_offset: Offset of the head entry.
        :param tail_offset: Offset of the tail entry (next added entry).
        :return: True if empty, False otherwise
        """
        return 0 == tail_offset - head_offset

    def enqueue(self, entry, txn_id, overwrite_if_full=False):
        """
        Append a new entry to the queue. Fails if the queue lock cannot be acquired. Can fail if the queue is full.
        If the fencing counter has wrapped around, reset all fencing values.
        :param entry: new entry to be enqueued
        :param txn_id: lock owner id, must be unique among concurrent requests
        :param overwrite_if_full: flag indicating if the head position should be overwritten if the queue is full
        :return: Offset position of the enqueued entry. throws: ASAborted('Queue is full'), ASAborted('Timed out')
        """
        q_state = self._lock(txn_id, LargeQueue.Ops.Enqueue)
        # compute the record and list indices
        head_offset = long(q_state['head-offset'])
        tail_offset = long(q_state['tail-offset'])
        fencing_ctr = q_state['fencing-ctr']
        # if the fencing counter has a non-positive value, it has wrapped around past the max value; reset
        if fencing_ctr <= 0:
            self._reset_fencing_marks()
        buf_rec_index, entry_index = self._get_entry_location(tail_offset) # tail points to where the new entry will go
        entry_val = {'offset': tail_offset, 'entry': entry}
        queue_is_full = self._queue_is_full(head_offset, tail_offset)
        if queue_is_full and not overwrite_if_full:
            self._commit_release(txn_id)
            raise ASAborted('Queue is full')
        predexps = [ predexp.integer_bin("fencing-mark"),
                     predexp.integer_value(fencing_ctr),
                     predexp.integer_less() ]
        ops = [ op_helpers.write('fencing-mark', fencing_ctr),
                list_helpers.list_set('entries', entry_index, entry_val) ]
        buf_rec_key = (self.namespace, self.name, LargeQueue._buf_record_key(buf_rec_index))
        try:
            (_, _, record) = self.client.operate(buf_rec_key, ops, policy={'predexp': predexps})
        except exception.FilteredOut as ex:
            raise ASAborted('Timed out')
        self._commit_release(txn_id, new_head_offset=head_offset + 1 if queue_is_full else None,
                             new_tail_offset=tail_offset + 1)
        return tail_offset

    def dequeue(self, txn_id):
        """
        Dequee and return the entry at the head of the queue. If the queue is empty, returns None.
        :param txn_id: lock owner id, must be unique among concurrent requests
        :return: dict containing entry and offset for the entry at the head of the queue,
            or None if the queue is empty
        """
        q_state = self._lock(txn_id, LargeQueue.Ops.Dequeue)
        # compute the record and list indices
        head_offset = long(q_state['head-offset'])
        tail_offset = long(q_state['tail-offset'])
        if self._queue_is_empty(head_offset, tail_offset):
            self._commit_release(txn_id)
            return None
        buf_rec_index, entry_index  = self._get_entry_location(head_offset)
        buf_key = (self.namespace, self.name, LargeQueue._buf_record_key(buf_rec_index))
        ops = [ list_helpers.list_get('entries', entry_index) ]
        (_, _, record) = self.client.operate(buf_key, ops)
        self._commit_release(txn_id, new_head_offset=head_offset+1)
        return record['entries']

    def get_entry_at_offset(self, offset):
        '''
        Get the entry at the given offset if the offset currently exists in the queue. The function
        does not acquire the queue lock and offers no guarantee that an entry exists or has not been removed
        at that offset.
        :param offset: offset of the entry (offset is the monotonically increasing position in the queue)
        :return: dict containing entry and offset if the entry at the offset is present in the queue, otherwise None
        '''
        # get head and tail offsets
        metadata_key = (self.namespace, self.name, LargeQueue.META_REC_KEY)
        metadata_bins = ['head-offset', 'tail-offset']
        (_, _, q_state) = self.client.select(metadata_key, metadata_bins)
        head_offset = long(q_state['head-offset'])
        tail_offset = long(q_state['tail-offset'])
        if (offset >= tail_offset) or (offset < head_offset):
            return None
        buf_rec_index, entry_index = self._get_entry_location(offset)
        buf_key = (self.namespace, self.name, LargeQueue._buf_record_key(buf_rec_index))
        entry = self.client.list_get(buf_key, 'entries', entry_index)
        if entry['offset'] != offset:
            return None
        return entry

# Usage and test examples
def main():
    '''
    Simple usage examples and tests.
    '''
    config = {
        'hosts': [('172.28.128.4', 3000)],
        'policies': {'timeout': 1200}
    }

    QUEUE_MAX_SIZE = 1000
    ENTRIES_PER_REC = 100
    client = aerospike.client(config).connect()

    # Create a new queue or initialize an existing queue
    msg_queue = LargeQueue()
    try:
        msg_queue.initialize_existing_queue(client, 'test', 'shared-msg-bus')
    except ASAborted as ex:
        msg_queue.create_new_queue(client, 'test', 'shared-msg-bus', QUEUE_MAX_SIZE, ENTRIES_PER_REC)

    # Add entries
    # add two messages
    # note txn-id is used as lock owner id, and needs only to be unique across all active clients
    # so we can use the same txn_id in the following operations
    txn_id = 1
    msg = {'msg-id': 100, 'msg-text': 'The school is closed for summer.'}
    offset = msg_queue.enqueue(msg, txn_id)
    print('message 100 added at offset {}'.format(offset))

    msg = {'msg-id': 101, 'msg-text': 'Have a nice summer!'}
    offset = msg_queue.enqueue(msg, txn_id)
    print('message 101 added at offset {}'.format(offset))

    # Get queue status
    q_info = msg_queue.get_queue_info()
    print('queue status: {}'.format(q_info))

    # Read entry at offset
    offset = q_info['head-offset'] + 1
    msg_entry = msg_queue.get_entry_at_offset(offset)
    print('entry at offset {}: {}'.format(offset, msg_entry))

    # Pop the head entry
    msg_entry = msg_queue.dequeue(txn_id)
    print('dequeued entry: {}'.format(msg_entry))

    # end status of the queue
    q_info = msg_queue.get_queue_info()
    print('end status: {}'.format(q_info))

    # TESTS
    # create a queue with max size 10 with 3 entries per record
    client = aerospike.client(config).connect()
    test_queue = LargeQueue()
    try:
        test_queue.initialize_existing_queue(client, 'test', 'test_queue')
    except ASAborted as ex:
        test_queue.create_new_queue(client, 'test', 'test_queue', 10, 3)

    # dequeue - empty queue
    txn_id = 111
    entry = test_queue.dequeue(txn_id)
    print('found: {}'.format(entry))

    # enqueue/dequeue
    entry = 999
    offset = test_queue.enqueue(entry, txn_id)
    out = test_queue.dequeue(txn_id)
    print('added: {} at offset: {}, dequeued: {}'.format(entry, offset, out))

    # add 20 items
    print('adding without overwrite')
    for i in range(20):
        try:
            offset = test_queue.enqueue(i, txn_id)
            print('added entry {} at offset {}'.format(i, offset))
        except ASAborted as ex:
            print('aborted: entry {}, reason: {}'.format(i, ex.reason))

    print('adding with overwrite')
    for i in (range(20)):
        try:
            offset = test_queue.enqueue(i, txn_id, True)
            print('added entry {} at offset {}'.format(i, offset))
        except ASAborted as ex:
            print('aborted: entry {}, reason: {}'.format(i, ex.reason))

    print('get info')
    info = test_queue.get_queue_info()
    print('info: {}'.format(info))

    print('get entries at offset')
    for i in range(info['head-offset'], info['tail-offset']):
        entry = test_queue.get_entry_at_offset(i)
        print('at offset {} got entry {}'.format(i, entry))

    print('dequeue all entries')
    while True:
        try:
            entry = test_queue.dequeue(txn_id)
            print('dequeued entry: {}'.format(entry))
            if entry is None:
                print('done')
                break
        except ASAborted as ex:
            print('aborted: reason: {}'.format(ex.reason))
            break
    exit(0)


if __name__ == "__main__":
    main()
    
    
