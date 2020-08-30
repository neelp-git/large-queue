# Large Queues in Aerospike

The repository provides an illustrative implementation of large queues in Aerospike

## Issues

The LargeQueue implementation provides a rough solution for certain issues when implementing a FIFO queue or a circular buffer
in Aerospike.

1. Large queue size. The List data type in Aerospike can be easily leveraged to implement a queue. However
    the maximum Aerospike record size is 8MB which is the outer limit of the queue size thus implemented. 
    LargeQueue allows an arbitrarily sized queue by using multiple records. 
2. Consistency across multiple records. In Aerospike, single record operations are atomic. Queue operations 
    are performed under a queue lock to preserve consistency across multiple records.
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
    
## Design
    
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
 
## Potential future enhancements
    
- Allow many consumer groups each with its own tracking metadata.
- Separate namepaces to store metatdata and buffer records. Performance can benefit by storing the metadata 
    namspace in faster persistent storage (like PMEM).
 - Extend metadata record to hold recent committed transactions for the ability to ascertain status by 
    transaction-id when various failures make an operation outcome unknown and the operation cannot  
    be simply resubmitted.
       
