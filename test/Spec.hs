{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}

module Main (main, spec) where

import           Test.Hspec
import           Control.Concurrent.Lifted
import           Control.Distributed.Process.MonadBaseControl()
import           Control.Distributed.Process hiding (bracket)
import           Control.Distributed.Process.Zookeeper
import           Control.Distributed.Process.Closure
import           Control.Distributed.Process.Serializable (Serializable)
import Control.Distributed.Process.Node (initRemoteTable)
import Control.DeepSeq (NFData)
import Control.Monad.Trans.Control (MonadBaseControl)
import Control.Monad (void)
import Control.Monad.IO.Class (MonadIO)
import Control.Exception.Enclosed (tryAnyDeep)
import Control.Exception.Lifted (throw)

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
                testBootWith defaultConfig {registerPrefix = "zk:"}  $  do
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
                    let cu =
                            returnCP sdictUnit ()
                            `bindCP` cpSend sdictUnit pid
                            `seqCP` cpExpect sdictUnit
                    let racer mv =
                           do void $ registerCandidate "test-global" cu
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
                    let cu =
                            returnCP sdictUnit ()
                            `bindCP` cpSend sdictUnit pid
                            `seqCP` cpExpect sdictUnit

                    let racer mv =
                           do gpid <- registerCandidate "test-global2" cu
                              putMVar mv gpid
                              expect :: Process ()

                    one <- forkSlave racer
                    two <- forkSlave racer
                    three <- forkSlave racer
                    gpid <- takeMVar one
                    void $ takeMVar two
                    void $ takeMVar three
                    exit gpid "because"
                    -- make sure only ONE candidate is elected on exit of
                    -- first
                    () <- expect :: Process ()
                    () <- expect :: Process ()
                    expectTimeout 100000
                `shouldReturn` (Nothing :: Maybe ())

zookeepers = "localhost:2181"

forkSlave ma =
 do mv <- newEmptyMVar
    void $ fork $ testBoot (ma mv)
    return mv

testBoot :: (MonadBaseControl IO io, MonadIO io, NFData a)
         => Process a -> io a
testBoot = testBootWith defaultConfig -- {logTrace = sayTrace}

testBootWith :: (MonadBaseControl IO io, MonadIO io, NFData a)
             => Config -> Process a -> io a
testBootWith config ma = testProcessTimeout 1000 (waitController >> ma >>= waitReturn )
                                      (liftIO . bootstrapWith config
                                                              "localhost"
                                                              "0"
                                                              zookeepers
                                                              initRemoteTable)

waitReturn a = do threadDelay 100000; return a

testProcessTimeout :: (NFData a, MonadBaseControl IO io)
                => Int -> Process a -> (Process () -> io ()) -> io a
testProcessTimeout timeout ma runProc = do
    resultMV <- newEmptyMVar
    void . fork $ runProc $ do
        eresult <- tryAnyDeep ma
        case eresult of
            Right result -> putMVar resultMV result
            Left exception -> putMVar resultMV (throw exception)
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

