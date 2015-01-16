{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE RecordWildCards #-}

module Control.Distributed.Process.Zookeeper
   ( zkController
   , registerZK
   ) where

import Database.Zookeeper.Lifted hiding (State)

import Control.Monad (void, join, forM_)
import Control.Concurrent.MVar (newEmptyMVar, takeMVar, putMVar)
import Control.Exception (throwIO)
import Data.Binary (Binary, encode)
import Data.Typeable (Typeable)
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as BS
import GHC.Generics (Generic)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Monoid (mempty)
import Data.Maybe (fromMaybe)

import Control.Distributed.Process hiding (proxy)
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.MonadBaseControl ()



data Command = Register String ProcessId (SendPort (Either String ()))
             | NodeList [String]
             | Exit
    deriving (Show, Typeable, Generic)

data State = State
    {
      conn :: Zookeeper
    , nodes :: [String]
    -- service nodes to remove each pid from when it exits
    , monPids :: Map ProcessId [String]
    }
instance Show State where
    show State{..} = show ("nodes", nodes, "monPids", monPids)

instance Binary Command

-- | Starts a Zookeeper service process, and installs an MXAgent to
-- automatically register all local names in Zookeeper.
zkController :: String -- ^ The Zookeeper endpoint, as in 'Base.withZookeeper'
             -> Process ()
zkController services =
 do proxy <- spawnProxy
    zkpid <- spawnLocal $
     do pid <- getSelfPid
        withZookeeper services 1000 (Just $ watcher proxy pid) Nothing $ \rzh ->
            let loop st@State{..} =
                    let recvCmd = match $ \command -> case command of
                                            Exit -> terminate --TODO: this could be better
                                            _ -> handle st command
                        recvMon = match $ \(ProcessMonitorNotification _ dead _) ->
                                            reap st dead
                    in say (show st) >> receiveWait [recvCmd, recvMon] >>= loop
            in loop (State rzh [] mempty)
    register controller zkpid
  where
    reap st@State{..} pid =
        let names = fromMaybe [] (Map.lookup pid monPids)
        in do forM_ names $ \name ->
               do let node = servicesNode </> name </> pretty pid
                  result <- delete conn node Nothing
                  case result of
                    Left reason -> say $ "Error: "
                                         ++ show reason
                                         ++ " - failed to delete "
                                         ++ node
                    _ -> say $ "Deleted " ++ node
              return st{monPids = Map.delete pid monPids}
    handle st@State{..} (Register name rpid reply) =
     do let node = servicesNode </> name </> pretty rpid
        create conn (servicesNode </> name)
                    Nothing OpenAclUnsafe [] >>= assertNode name
        result <- create conn node (pidB rpid)
                         OpenAclUnsafe [Ephemeral]
        case result of
            Right _ ->
             do say $ "Registered " ++ node
                sendChan reply (Right ())
            Left reason -> sendChan reply (Left $ show reason)

        void $ monitor rpid
        return st{monPids = Map.insertWith (++) rpid [name] monPids}

    handle st (NodeList n) = return st{nodes = n}

    handle st Exit = return st

    watcher proxy pid rzh SessionEvent ConnectedState _ =
      void $ do
                create rzh rootNode Nothing OpenAclUnsafe [] >>= assertNode rootNode
                create rzh servicesNode Nothing OpenAclUnsafe [] >>= assertNode servicesNode
                create rzh controllersNode Nothing OpenAclUnsafe [] >>= assertNode controllersNode
                Right _ <- create rzh (controllersNode </> pretty pid)
                                      (pidB pid) OpenAclUnsafe [Ephemeral]
                Right children <- getChildren rzh controllersNode Nothing
                proxy $ castZK (NodeList children)

    watcher _ _ _ _ _ _ = return ()

    servicesNode = rootNode </> "services"
    controllersNode = rootNode </> "controllers"
    rootNode = "/distributed-process"
    l </> r = l ++ "/" ++ r
    pretty pid = drop 6 (show pid)
    pidB pid = Just . BS.concat . BL.toChunks $ encode pid

    assertNode _ (Right _) = return ()
    assertNode _ (Left NodeExistsError) = return ()
    assertNode name (Left _) = liftIO $
        throwIO (userError $ "Fatal: could not create node: " ++ name)

registerZK :: String -> ProcessId -> Process (Either String ())
registerZK name rpid =
    callZK $ Register name rpid

controller :: String
controller = "zookeeper"

callZK :: Serializable a => (SendPort a -> Command) -> Process a
callZK command =
    do Just pid <- whereis controller
       (sendCh, replyCh) <- newChan
       link pid
       send pid (command sendCh)
       result <- receiveChan replyCh
       unlink pid
       return result

castZK :: Command -> Process ()
castZK command =
    do Just pid <- whereis controller
       send pid command

spawnProxy :: Process (Process () -> IO ())
spawnProxy =
 do mv <- liftIO newEmptyMVar
    void $ spawnLocal $
        let loop = join (liftIO $ takeMVar mv) >> loop
        in loop
    return (putMVar mv)
