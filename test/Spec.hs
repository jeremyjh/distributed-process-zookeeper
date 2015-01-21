{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}

module Main (main, spec) where

import           Test.Hspec
import           Control.Concurrent.Lifted
import           Control.Distributed.Process.MonadBaseControl()
import           Control.Distributed.Process hiding (bracket)
import           Control.Exception (bracket)
import           Control.Distributed.Process.Zookeeper
import           Control.Distributed.Process.Serializable (Serializable)
import Control.Distributed.Process.Node (initRemoteTable)
import Control.DeepSeq (NFData)
import Control.Monad.Trans.Control (MonadBaseControl)
import Control.Monad (void)
import Control.Exception.Enclosed (tryAnyDeep)
import Control.Exception.Lifted (throw)

import Control.Distributed.Process.Node
       (newLocalNode, runProcess)
import Network (HostName)
import Network.Socket (ServiceName)
import Network.Transport.TCP
       (createTransport, defaultTCPParameters)
import Network.Transport (closeTransport)

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
    describe "Test harness" $ do

            it "can do basic Process messaging" $ do
                testBoot "3030" $  do
                    parent <- getSelfPid
                    child <- spawnLocal $ do
                        saysee <- texpect :: Process String
                        send parent ("I said: " ++ saysee)
                    send child "foo"
                    said <- texpect :: Process String
                    return said
                `shouldReturn` "I said: foo"

    describe "C.D.P. Zookeeper" $ do

            it "can do basic Process messaging" $ do
                testBoot "3030" $  do
                    parent <- getSelfPid
                    child <- spawnLocal $ do
                        saysee <- texpect :: Process String
                        send parent ("I said: " ++ saysee)
                    send child "foo"
                    said <- texpect :: Process String
                    return said
                `shouldReturn` "I said: foo"

zookeepers = "localhost:2181"


testBoot port ma = testProcessTimeout 1000 ma
                                      (liftIO . bootstrap "localhost"
                                                          port zookeepers
                                                          initRemoteTable)

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

bootstrap
          -- ^ Hostname or IP this Cloud Haskell node will listen on.
          :: HostName
             -- ^ Port or port name this node will listen on.
          -> ServiceName
             -- ^ The Zookeeper endpoint(s) -- comma separated list of host:port
          -> String
          -> RemoteTable
          -> Process () -- ^ Process computation to run in the new node.
          -> IO ()
bootstrap = bootstrapWith defaultConfig

bootstrapWith
          :: Config
          -- ^ Hostname or IP this Cloud Haskell node will listen on.
          -> HostName
             -- ^ Port or port name this node will listen on.
          -> ServiceName
             -- ^ The Zookeeper endpoint(s) -- comma separated list of host:port
          -> String
          -> RemoteTable -> Process () -> IO ()
bootstrapWith config host port zservs rtable proc =
    bracket acquire release exec
  where
    acquire =
     do Right tcp <- createTransport host port defaultTCPParameters
        return tcp
    exec tcp =
       do node <- newLocalNode tcp rtable
          runProcess node $
           do void $ spawnLocal (zkControllerWith config zservs)
              waitController
              proc
    release = closeTransport
