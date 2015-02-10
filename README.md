## Overview

This package provides a Zookeeper backend for Cloud Haskell. It supports the same API as the [distributed-process-p2p](http://hackage.haskell.org/package/distributed-process-p2p) package for basic service and node discovery tasks. It also takes advantage of Zookeeper primitives to support election and discovery of globally unique processes.

This Zookeeper back-end can bring a few advantages:

*  It is not necessary to connect to every node in the cluster in order to do service discovery. If Node A does not offer a service of interest to Node B, Node B need never connect to Node A.
*  Service data is cached locally (cache is cleared with Zookeeper watches) - multiple queries for the same service name are served from cache, unless processes advertising that service have been added or removed (cache is swept when Zookeeper watches fire.) 
*  Globally unique processes can be established via leader election.

The trade-off is effort to manage the Zookeeper cluster. One server is fine for development or casual deployments but for production you really need a cluster of five servers or else you've mostly increased the probability of failure.

## Global Processes via Leader Election

A principal advantage to using a distributed consensus server like Zookeeper is to support the ability to have exactly one instance of a particular service across an entire cluster, and to ensure that if the node supporting that instance fails that it will be replaced in an orderly fashion. Zookeeper provides primitives such as ephemeral nodes and sequences, along with [some recipes](http://zookeeper.apache.org/doc/trunk/recipes.html#sc_leaderElection) for their usage. 

This package takes advantage of these features and design patterns and provides an implementation for Cloud Haskell, that make it possible to develop systems similar to the below snippet (from [example](https://github.com/jeremyjh/distributed-process-zookeeper/blob/master/examples/Boss.hs)) which contains a single "boss" that will delegate tasks submitted by transient requestors to a pool of worker processes which are also candidates for taking over that work should the boss fail.
```Haskell
main =
  bootstrapWith defaultConfig --{logTrace = sayTrace}
                              "localhost"
                              "0"
                              "localhost:2181"
                              initRemoteTable $
     do args <- liftIO getArgs
        case args of
            job : _ -> -- transient requestor - sends job and exits
             do Just boss <- whereisGlobal "boss"
                send boss job
            _  -> -- will be a boss or boss candidate + worker
             do say "Starting persistent process - press <Enter> to exit."
                Right boss <- registerCandidate "boss" $
                                do say "I'm the boss!"
                                   workers <- getCapable "worker"
                                   case length workers of
                                       0 -> say "I don't do any work! Start a worker."
                                       n -> say $ "I've got " ++ show n ++ " workers right now."
                                   bossLoop 0
                self <- getSelfNode
                if self == processNodeId boss -- like a boss
                    then void $ liftIO getLine
                    else do worker <- getSelfPid
                            register "worker" worker
                            say "Worker waiting for task..."
                            workLoop
```

## Installing and running the examples.
GHC version 7.6.3 or higher is supported. The package uses the [hzk](http://hackage.haskell.org/package/hzk) bindings, which require the Zookeeper Multi-thread C library to be installed. You also need a Zookeeper server installed. A cabal sandbox is recommended for installation. 

On Ubuntu (tested on 14.04) you can install these dependencies, build and run the example with:

```bash
$ sudo apt-get install libzookeeper-mt-dev zookeeperd
$ sudo service zookeeper start 
$ cabal sandbox init
$ cabal install -j distributed-process-zookeeper -fzkexamples
$ .cabal-sandbox/bin/Boss

# now start some workers - open a new shell/window

$ .cabal-sandbox/bin/Boss

# and another

$ .cabal-sandbox/bin/Boss

# submit jobs - should see 'Delegation' message from first window and 'doing' from one of the workers

$ .cabal-sandbox/bin/Boss foo

# jobs will be distributed between workers

$ .cabal-sandbox/bin/Boss bar 

# now stop the original Boss session by pressing <Enter> in that window - the second process should activate its boss candidate and will both 'Delegate' and 'Do' tasks
```

## Status

Experimental. I have not yet used it with a production workload.
