## Overview

This package provides a Zookeeper back-end for Cloud Haskell. It supports the same API as the distributed-process-p2p package for basic service and node discovery tasks. It also takes advantage of Zookeeper primitives to support election and discovery of globally unique processes.

This Zookeeper back-end can bring a few advantages:

*  It is not necessary to connect to every node in the cluster in order to do service discovery. If Node A does not offer a service of interest to Node B, Node B need never connect to Node A.

*  Service data is cached locally (cache is cleared with Zookeeper watches) - multiple queries for the same service name are served from cache, unless processes advertising that service have been added or removed (cache is swept when Zookeeper watches fire.) 
*  Globally unique processes can be established via leader election.

The trade-off is managing the Zookeeper cluster. One server is fine for development but for production you really need a cluster of five servers or else you've increased the possiblity of failure.

## Global Processes via Leader Election

A principal advantage to using a distributed consensus server like Zookeeper is to support the ability to have exactly one instance of a particular service across an entire cluster, and to ensure that if the node supporting that instance fails that it will be replaced in an orderly fashion. Zookeeper provides primitives such as ephemeral nodes and sequences, along with [some recipes](http://zookeeper.apache.org/doc/trunk/recipes.html#sc_leaderElection) for their usage. 

This package takes advantage of these features and design patterns and provides an implementation for Cloud Haskell, that make it possible to develop systems similar to the below [example](https://github.com/jeremyjh/distributed-process-zookeeper/blob/master/examples/Boss.hs) which contains a single "boss" that will delegate tasks submitted by transient requestors to a pool of worker processes which are also candidates for taking over that work should the boss fail.

```Haskell
main =
  bootstrapWith defaultConfig --{logTrace = sayTrace}
                              "localhost"
                              "0"
                              "localhost:2181"
                              initRemoteTable $
     do args <- liftIO getArgs
        case args of
            job : _ -> -- arg job name - transient requestor - sends job and exits
             do Just boss <- whereisGlobal "boss"
                send boss job
            _  ->     -- no args - persistent process - will be a boss or boss candidate + worker
             do Right boss <- registerCandidate "boss" $
                                  say "I'm the boss!" >> bossLoop 0
                self <- getSelfNode
                if self == processNodeId boss -- like a boss
                    then do say "I don't do any work! Start a worker."
                            void $ liftIO getLine
                            return ()
                    else do worker <- getSelfPid
                            register "worker" worker
                            say "Worker waiting for task..."
                            workLoop
```

## Status

Experimental. I have not yet used it with a production workload. The caching functionality makes up about 75% of the complexity budget and yet it probably does not have adequate test coverage.
