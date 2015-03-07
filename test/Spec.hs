{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}

module Main (main, spec) where

import           Test.Hspec
import           Control.Concurrent.Lifted
import           Control.Distributed.Process.MonadBaseControl()
import           Control.Distributed.Process hiding (bracket)
import           Control.Distributed.Process.Zookeeper
import           Control.Distributed.Process.Serializable (Serializable)
import qualified Database.Zookeeper as ZK
import Control.Distributed.Process.Node (initRemoteTable)
import Control.DeepSeq (NFData, deepseq)
import Control.Monad.Trans.Control (MonadBaseControl)
import Control.Monad (void)
import Control.Monad.IO.Class (MonadIO)
import Control.Exception.Enclosed (catchAny)
import Control.Exception.Lifted (throw)
import Data.ByteString.Char8 (pack)

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
    describe "Test harness" $ do

            it "can do basic Process messaging" $ do
                testBoot $  do
                    parent <- getSelfPid
                    child <- spawnLocal $ do
                        saysee <- texpect :: Process String
                        send parent ("I said: " ++ saysee)
                    send child "foo"
                    said <- texpect :: Process String
                    return said
                `shouldReturn` "I said: foo"

    describe "C.D.P. Zookeeper" $ do

            it "will register local names with zookeeper" $ do
                testBoot $  do
                    self <- getSelfPid
                    register "testy" self
                    threadDelay 10000
                    [found] <- getCapable "testy"
                    return (found == self)
                `shouldReturn` True

            it "will only register prefixed local names if configured" $ do
                testBootWith defaultConfig {registerPrefix = "zk:"} zookeepers  $  do
                    self <- getSelfPid
                    register "testr" self
                    register "zk:testr2" self
                    threadDelay 10000
                    [] <- getCapable "testr"
                    [found] <- getCapable "zk:testr2"
                    return (found == self)
                `shouldReturn` True

            it "enables broadcast to specific services" $ do
                testBoot $ do
                    one <- newEmptyMVar
                    two <- newEmptyMVar
                    let slave mv = void $ fork $ testBoot $
                           do self <- getSelfPid
                              register "test-broadcast" self
                              threadDelay 50000 --finish registration
                              putMVar mv ()
                              "foundme" <- expect
                              putMVar mv ()
                              expect :: Process ()
                    slave one
                    slave two
                    takeMVar one
                    takeMVar two --hopefully they are both registered by now!
                    nsendCapable "test-broadcast" "foundme"
                    takeMVar one
                    takeMVar two
                `shouldReturn` ()

            it "enables broadcast to all nodes" $ do
                testBoot $ do
                    let slave mv =
                           do self <- getSelfPid
                              register "test-broadcast" self
                              threadDelay 50000 --finish registration
                              putMVar mv ()
                              "foundme" <- expect
                              putMVar mv ()
                    one <- forkSlave slave
                    two <- forkSlave slave
                    takeMVar one
                    takeMVar two --hopefully they are both registered by now!
                    nsendPeers "test-broadcast" "foundme"
                    takeMVar one
                    takeMVar two
                `shouldReturn` ()

            it "will run a single process with a global name" $ do
                testBoot $ do
                    pid <- getSelfPid
                    let racer mv =
                           do void $ registerCandidate "test-global" $
                                      do send pid ()
                                         expect :: Process ()
                              putMVar mv ()
                              expect :: Process ()

                    one <- forkSlave racer
                    two <- forkSlave racer
                    takeMVar one
                    takeMVar two
                    -- we should only get one
                    () <- expect :: Process ()
                    expectTimeout 100000
                `shouldReturn` (Nothing :: Maybe ())

            it "will elect a new global when one exits" $ do
                testBoot $ do
                    pid <- getSelfPid

                    let racer mv =
                           do Right gpid <- registerCandidate "test-global2" $
                                              do send pid ()
                                                 expect :: Process ()
                              putMVar mv gpid
                              expect :: Process ()

                    one <- forkSlave racer
                    two <- forkSlave racer
                    three <- forkSlave racer
                    first <- takeMVar one
                    void $ takeMVar two
                    void $ takeMVar three
                    exit first "because"
                    () <- expect :: Process ()
                    () <- expect :: Process ()
                    Just second <- whereisGlobal "test-global2"
                    -- make sure only ONE candidate is elected on exit of
                    -- first
                    Nothing <- expectTimeout 100000 :: Process (Maybe ())
                    return (first /= second)
                `shouldReturn` True

            it "will not leak processes when a candidate is attempted to register again" $ do
                testBoot $ do
                    nodeid <- getSelfNode

                    Right gpid <- registerCandidate "test-global3" $
                                      (expect :: Process ())

                    Right stats <- getNodeStats nodeid
                    let b4 = nodeStatsProcesses stats

                    Right same <- registerCandidate "test-global3" $
                                      (expect :: Process ())

                    Right stats' <- getNodeStats nodeid
                    let after' = nodeStatsProcesses stats'

                    return ((b4 + 1 == after') && (gpid == same))
                `shouldReturn` True

            it "will clear the name cache when a service exits" $ do
                testBoot $ do
                    parent <- getSelfPid
                    void $ spawnLocal $
                             do self <- getSelfPid
                                register "test-cache" self
                                threadDelay 10000
                                send parent ()
                                (expect :: Process ())
                                send parent ()
                    (expect :: Process ())
                    [pid] <- getCapable "test-cache"
                    send pid ()
                    (expect :: Process ())
                    threadDelay 10000
                    getCapable "test-cache"
                `shouldReturn` []

            it "will clear the name cache when a global process exits" $ do
                testBoot $ do
                    Right gpid <- registerCandidate "test-cache2" $
                                      (expect :: Process ())
                    send gpid ()
                    threadDelay 100000
                    whereisGlobal "test-cache2"
                `shouldReturn` Nothing

            describe "supports authorization" $ do

                it "can setup with restricted acl" $ do
                    testBootAuth "user:rightpassword" $ do
                        -- we just run this for the initialization with
                        -- the "correct" credentials and restricted ACL
                        return ()
                    `shouldReturn` ()

                it "will fail if there is an ACL with different credentials" $ do
                    testBootAuth "user:wrongpassword" $ do
                        return ()
                    `shouldThrow` linkException

linkException :: ProcessLinkException -> Bool
linkException _ = True

zookeepers = "localhost:2181"

forkSlave ma =
 do mv <- newEmptyMVar
    void $ fork $ testBoot (ma mv)
    return mv

testBoot :: (MonadBaseControl IO io, MonadIO io, Show a, NFData a)
         => Process a -> io a
testBoot = testBootWith defaultConfig zookeepers -- {logTrace = sayTrace}

testBootAuth :: (MonadBaseControl IO io, MonadIO io, Show a, NFData a)
             => String -> Process a -> io a
testBootAuth creds = testBootWith defaultConfig { credentials = Just ("digest", pack creds)
                                          , acl = ZK.CreatorAll
                                          }
                            (zookeepers ++ "/testauth")

testBootWith :: (MonadBaseControl IO io, MonadIO io, Show a, NFData a)
             => Config -> String -> Process a -> io a
testBootWith config servers ma = testProcessTimeout 1000 (ma >>= waitReturn )
                                      (liftIO . bootstrapWith config
                                                              "localhost"
                                                              "0"
                                                              servers
                                                              initRemoteTable)

waitReturn a = do threadDelay 100000; return a

testProcessTimeout :: (Show a, NFData a, MonadBaseControl IO io)
                => Int -> Process a -> (Process () -> io ()) -> io a
testProcessTimeout timeout ma runProc = do
    resultMV <- newEmptyMVar
    void . fork $
        catchAny (runProc $
                    do a <- ma
                       a `deepseq` putMVar resultMV a)
                 (\e -> putMVar resultMV (throw e))
    !result <- waitMVar resultMV
    return result
  where
    waitMVar mv = loop 0
      where loop lapsed
              | lapsed < timeout =
                  tryTakeMVar mv >>= maybe (delay >> loop (lapsed + 1))
                                           return
              | otherwise = error "Execution of Process test timed-out."
            delay = threadDelay 1000

texpect :: Serializable a => Process a
texpect = do
    gotit <- expectTimeout 500000 -- 500ms may as well be never
    case gotit of
        Nothing -> error "Timed out in test expect"
        Just v -> return v
