# Python Hydra

Hydra is a Python implementation of various capabilities used for multi-proces sand multi-host distributed processing and logic.
It is design to encompass more and more functionality over time, as new common use cases are 
identified.

## Hydra Distributed Queues

Hydra distribute queues are queues that can be accessed across multiple hosts.  There are two types
of queues. First,  a *source queue*, where a single host serves a queue that allows a client to populate a common
queue on one host, and allow other processes and hosts to get (pull) from this common queue.  Secondly, 
a *sink queue*, where a single host serves a queue that allows other remote clients to put (push) items 
into the common queue.  These are essentially mirrors of each other. 

### Motivation
Hydra distributed queues were motivated for use in multi-host, distributed Python testing.  A single queue 
is created and populated with tests to be executed. Remote clients then pull from this queue to get the next
test.  Only one test is served to one client -- the one that happens to request the next item in the queue. 

Likewise, the remote clients can push results to a single sink-queue for the main process to read and collate
test results in a single place.

This type of set up allows for very efficient execution.  As each client completes one test, it simply pulls the
next available test for execution with minimal delay.  The queues are joinable queues and the source-queue is
a task-based queue that allows signaling start and end of each test executed.  
