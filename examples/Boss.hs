import Control.Distributed.Process
import Control.Distributed.Process.Node (initRemoteTable)
import Control.Distributed.Process.Zookeeper
import Control.Monad (void)
import System.Environment (getArgs)

-- Run the program with "args" to request a job named "args" - run with no
-- args to startup a boss and each worker. Entering a new-line will
-- terminate Boss or Workers.
main :: IO ()
main =
  bootstrapWith defaultConfig --{logTrace = sayTrace}
                              "localhost"
                              "0"
                              "localhost:2181"
                              initRemoteTable $
     do args <- liftIO getArgs
        case args of
            job : more  -> -- transient requestor - sends job and exits
             do Just boss <- whereisGlobal "boss"
                send boss (job ++ " " ++ unwords more)
            _  -> -- will be a boss or boss candidate + worker
             do say "Starting persistent process - press <Enter> to exit."
                Right boss <- registerCandidate "boss" $
                                do say "I'm the boss!"
                                   workers <- getCapable "worker"
                                   case length workers of
                                       0 -> say $ "I don't do any work! Start "
                                                ++ " a worker."
                                       n -> say $ "I've got " ++ show n
                                                ++ " workers right now."
                                   bossLoop 0
                self <- getSelfNode
                if self == processNodeId boss -- like a boss
                    then void $ liftIO getLine
                    else do worker <- getSelfPid
                            register "worker" worker
                            say "Worker waiting for task..."
                            void . spawnLocal $ --wait for exit
                                do void $ liftIO getLine
                                   send worker "stop"
                            workLoop

-- Calling getCapable for every loop is no problem because the results
-- of this are cached until the worker node is updated in Zookeeper.
bossLoop :: Int -> Process ()
bossLoop i =
 do job <- expect :: Process String
    found <- getCapable "worker"
    case found of
        [] ->
         do say "Where is my workforce?!"
            bossLoop i
        workers ->
         do say $ "'Delegating' task: " ++ job ++ " to " ++ show (pick workers)
            send (pick workers) job >> bossLoop (i + 1)
  where pick workers
          | length workers > 1 = head $ drop (mod i (length workers)) workers
          | otherwise = head workers

workLoop :: Process ()
workLoop =
  do work <- expect :: Process String
     case take 4 work of
        "stop" ->
            say "No more work for me!"
        _ ->
         do say $ "Doing task: " ++ work
            workLoop
