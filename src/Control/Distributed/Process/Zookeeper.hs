{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE RecordWildCards    #-}

module Control.Distributed.Process.Zookeeper
   ( zkController
   , registerZK
   , getPeers
   , getCapable
   , nsendPeers
   , nsendCapable
   ) where

import           Database.Zookeeper                       (AclList (..),
                                                           CreateFlag (..),
                                                           Event (..),
                                                           ZKError (..),
                                                           Zookeeper,
                                                           Watcher)
import qualified Database.Zookeeper                       as ZK

import Control.Concurrent.MVar
       (newEmptyMVar, putMVar, takeMVar, putMVar, takeMVar)
import           Control.Exception                        (throwIO)
import           Control.Monad                            (forM, forM_, join,
                                                           void)
import           Control.Monad.Except                     (ExceptT (..))
import           Control.Monad.IO.Class                   (MonadIO)
import           Control.Monad.Trans.Except               (runExceptT, throwE)
import           Data.Binary                              (Binary, decode,
                                                           encode)
import qualified Data.ByteString                          as BS
import qualified Data.ByteString.Lazy                     as BL
import           Data.Map.Strict                          (Map)
import qualified Data.Map.Strict                          as Map
import           Data.Maybe                               (fromMaybe)
import           Data.Monoid                              (mempty)
import           Data.Typeable                            (Typeable)
import           GHC.Generics                             (Generic)

import           Control.Distributed.Process              hiding (proxy)
import           Control.Distributed.Process.Serializable

data Command = Register String ProcessId (SendPort (Either String ()))
             | ClearCache String
             | GetRegistered String (SendPort [ProcessId])
             | GetControllerNodeIds (SendPort [NodeId])
             | Exit
    deriving (Show, Typeable, Generic)

data State = State
    {
      nodeCache :: Map String [ProcessId]
    -- service nodes to remove each pid from when it exits
    , monPids :: Map ProcessId [String]
    , spid :: ProcessId
    , proxy :: Process () -> IO ()
    }

instance Show State where
    show State{..} = show ("nodes", nodeCache, "monPids", monPids)

instance Binary Command

-- |Register a name and pid a as service in Zookeeper.
--
-- Names will be registered at /distributed-process/services/<name>/<pid>
registerZK :: String -> ProcessId -> Process (Either String ())
registerZK name rpid =
    callZK $ Register name rpid

-- | Get a list of nodes advertised in Zookeeper. These pids are registered
-- when zkController starts in path
-- "/distributed-process/controllers/<pid>".
getPeers :: Process [NodeId]
getPeers = callZK GetControllerNodeIds

-- | Returns list of pids registered with the service name.
getCapable :: String -> Process [ProcessId]
getCapable = callZK . GetRegistered

-- | Broadcast a message to a specific service on all registered nodes.
nsendPeers :: Serializable a => String -> a -> Process ()
nsendPeers service msg = getPeers >>= mapM_ (\peer -> nsendRemote peer service msg)

-- | Broadcast a message to a all pids registered with a particular service
-- name.
nsendCapable :: Serializable a => String -> a -> Process ()
nsendCapable service msg = getCapable service >>= mapM_ (`send` msg)

-- | Starts a Zookeeper service process, and installs an MXAgent to
-- automatically register all local names in Zookeeper.
zkController
             -- ^ The Zookeeper endpoint(s) -- comma separated list of host:port
             :: String
             -> Process ()
zkController keepers =
 do run <- spawnProxy
    liftIO $ ZK.withZookeeper keepers 1000 (Just inits) Nothing $ \rzh ->
        run (server rzh)
  where
    inits rzh SessionEvent ZK.ConnectedState _ =
      void $
       do create rzh rootNode Nothing OpenAclUnsafe [] >>= assertNode rootNode
          create rzh servicesNode Nothing OpenAclUnsafe [] >>= assertNode servicesNode
          create rzh controllersNode Nothing OpenAclUnsafe [] >>= assertNode controllersNode

    inits _ _ _ _ = return ()

server :: Zookeeper -> Process ()
server rzh =
 do pid <- getSelfPid
    register controller pid
    proxy' <- spawnProxy
    Right _ <- create rzh (controllersNode </> pretty pid)
                          (pidB pid) OpenAclUnsafe [Ephemeral]
    let loop st =
          let recvCmd = match $ \command -> case command of
                                  Exit -> return ()
                                  _ -> handle st command >>= loop
              recvMon = match $ \(ProcessMonitorNotification _ dead _) ->
                                  reap st dead >>= loop
          in say (show st) >> receiveWait [recvCmd, recvMon]
    void $ loop (State mempty mempty pid proxy')
  where
    watchCache State{..} _ ChildEvent ZK.ConnectedState (Just node) =
        proxy $ send spid (ClearCache node)

    watchCache _ _ _ _ _ = return ()

    reap st@State{..} pid =
        let names = fromMaybe [] (Map.lookup pid monPids)
        in do forM_ names $ \name ->
               do let node = servicesNode </> name </> pretty pid
                  result <- delete rzh node Nothing
                  case result of
                    Left reason -> say $ "Error: "
                                         ++ show reason
                                         ++ " - failed to delete "
                                         ++ node
                    _ -> say $ "Reaped " ++ node
              return st{monPids = Map.delete pid monPids}

    handle st@State{..} (Register name rpid reply) =
     do let node = servicesNode </> name </> pretty rpid
        create rzh (servicesNode </> name)
                    Nothing OpenAclUnsafe [] >>= assertNode name
        result <- create rzh node (pidB rpid)
                         OpenAclUnsafe [Ephemeral]
        case result of
            Right _ ->
             do say $ "Registered " ++ node
                sendChan reply (Right ())
            Left reason -> sendChan reply (Left $ show reason)

        void $ monitor rpid
        return st{monPids = Map.insertWith (++) rpid [name] monPids}

    handle st@State{..} (GetControllerNodeIds reply) =
     do epids <- case Map.lookup controllersNode nodeCache of
                        Just pids -> return (Right pids)
                        Nothing -> getChildPids rzh controllersNode (Just $ watchCache st)
        case epids of
            Right pids ->
             do sendChan reply (map processNodeId pids)
                return st{nodeCache = Map.insert controllersNode pids nodeCache}
            Left reason ->
             do say $  "Retrieval failed for controllers: " ++ reason
                sendChan reply []
                return st{nodeCache = Map.delete controllersNode nodeCache}

    handle st@State{..} (ClearCache node) =
        return st{nodeCache = Map.delete node nodeCache}

    handle st@State{..} (GetRegistered node reply) =
        let node' = servicesNode </> node in
     do epids <- case Map.lookup node' nodeCache of
                        Just pids -> return (Right pids)
                        Nothing -> getChildPids rzh node' (Just $ watchCache st)
        case epids of
            Right pids ->
             do sendChan reply pids
                return st{nodeCache = Map.insert node pids nodeCache}
            Left reason ->
             do say $  "Retrieval failed for node: " ++ node' ++ " - " ++ reason
                sendChan reply []
                return st{nodeCache = Map.delete node nodeCache}

    handle st Exit = return st

    pretty pid = drop 6 (show pid)
    pidB pid = Just . BL.toStrict $ encode pid


getChildPids :: MonadIO m
             => Zookeeper -> String -> Maybe Watcher
             -> m (Either String [ProcessId])
getChildPids rzh node watcher = liftIO $
 do enodes' <- ZK.getChildren rzh node watcher
    runExceptT $
     do children <- hoistEither (showEither enodes')
        forM children $ \child ->
         do eresult <- liftIO $ ZK.get rzh (node </> child) Nothing
            case eresult of
               Left reason ->
                 do let msg = "Error fetching data for " ++ (node </> child)
                              ++ " : " ++ show reason
                    throwE msg
               Right (Nothing, _) ->
                 do let msg = "Error fetching data for " ++ (node </> child)
                              ++ " : data was empty."
                    throwE msg
               Right (Just bs, _) -> return (decode $ BL.fromStrict bs)

hoistEither :: Monad m => Either e a -> ExceptT e m a
hoistEither = ExceptT . return

showEither :: Show a => Either a b -> Either String b
showEither (Left zkerr) = Left $ show zkerr
showEither (Right a) = Right a

(</>) :: String -> String -> String
l </> r = l ++ "/" ++ r

servicesNode :: String
servicesNode = rootNode </> "services"

controllersNode :: String
controllersNode = rootNode </> "controllers"

rootNode :: String
rootNode = "/distributed-process"

assertNode :: MonadIO m => String -> Either ZKError t -> m ()
assertNode _ (Right _) = return ()
assertNode _ (Left NodeExistsError) = return ()
assertNode name (Left _) = liftIO $
    throwIO (userError $ "Fatal: could not create node: " ++ name)

controller :: String
controller = "zookeeper:controller"

callZK :: Serializable a => (SendPort a -> Command) -> Process a
callZK command =
    do Just pid <- whereis controller
       (sendCh, replyCh) <- newChan
       link pid
       send pid (command sendCh)
       result <- receiveChan replyCh
       unlink pid
       return result

-- | Create a process runner that communicates
-- through an MVar - so you can call process actions from IO
spawnProxy :: Process (Process a -> IO a)
spawnProxy =
 do action <- liftIO newEmptyMVar
    result <- liftIO newEmptyMVar
    void $ spawnLocal $
        let loop = join (liftIO $ takeMVar action)
                   >>= liftIO . putMVar result
                   >> loop in loop
    return $ \f -> putMVar action f >> takeMVar result

create :: MonadIO m => Zookeeper -> String -> Maybe BS.ByteString
       -> AclList -> [CreateFlag] -> m (Either ZKError String)
create z n d a = liftIO . ZK.create z n d a

delete :: MonadIO m => Zookeeper -> String -> Maybe ZK.Version -> m (Either ZKError ())
delete z n = liftIO . ZK.delete z n
