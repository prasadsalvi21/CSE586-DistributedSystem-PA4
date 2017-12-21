this assignment is about implementing a simplified version of Dynamo. (And
you might argue that it’s not Dynamo any more ;-) There are three main pieces you need to
implement: 1) Partitioning, 2) Replication, and 3) Failure handling.
The main goal is to provide both availability and linearizability at the same time. In other words,
your implementation should always perform read and write operations successfully even under
failures. At the same time, a read operation should always return the most recent value.