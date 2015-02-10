import Control.Distributed.Process
import Control.Distributed.Process.Node (initRemoteTable)
import Control.Distributed.Process.Zookeeper
import Control.Monad (void)
import System.Environment (getArgs)
import System.Timeout (timeout)

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

bossLoop :: Int -> Process ()
bossLoop i =
 do job <- expect :: Process String
    say $ "'Delegating' task: " ++ job
    found <- getCapable "worker"
    case found of
        [] ->
         do say "Where is my workforce?!"
            bossLoop i
        workers ->
            send (pick workers) job >> bossLoop (i + 1)
  where pick workers
          | length workers > 1 = head $ drop (mod i (length workers)) workers
          | otherwise = head workers

workLoop :: Process ()
workLoop =
  do mwork <- expectTimeout 100 :: Process (Maybe String)
     case mwork of
        Just work ->
          do say $ "Doing task: " ++ work
             workLoop
        Nothing ->
         do mquit <- liftIO $ timeout 1000 getLine
            case mquit of
                Nothing -> workLoop
                Just _ -> return ()
