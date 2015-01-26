{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE RecordWildCards    #-}

module Control.Distributed.Process.Zookeeper
   (
     zkController
   , zkControllerWith
   , bootstrap
   , bootstrapWith
   , registerZK
   , getPeers
   , getCapable
   , nsendPeers
   , nsendCapable
   , waitController
   , Config(..)
   , defaultConfig
   , nolog
   , sayTrace
   ) where

import           Database.Zookeeper                       (AclList (..),
                                                           CreateFlag (..),
                                                           Event (..),
                                                           ZKError (..),
                                                           Zookeeper,
                                                           Watcher)
import qualified Database.Zookeeper                       as ZK

import Control.Concurrent
       (threadDelay, newEmptyMVar, putMVar, takeMVar, putMVar, takeMVar)
import           Control.Exception                        (throwIO, bracket)
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
import Data.List (isPrefixOf)
import           GHC.Generics                             (Generic)
import Control.Applicative ((<$>))
import Control.DeepSeq (deepseq)

import           Control.Distributed.Process              hiding (proxy, bracket)
import           Control.Distributed.Process.Management
import           Control.Distributed.Process.Serializable
import Control.Distributed.Process.Node
       (newLocalNode, runProcess)
import Network (HostName)
import Network.Socket (ServiceName)
import Network.Transport.TCP
       (createTransport, defaultTCPParameters)
import Network.Transport (closeTransport)


data Command = Register String ProcessId (SendPort (Either String ()))
             | ClearCache String
             | GetRegistered String (SendPort [ProcessId])
             | Exit
    deriving (Show, Typeable, Generic)

instance Binary Command

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

data Config = Config
    {
      -- | Only register locally registered process names with zookeeper
      -- if the name begins with the given prefix. Default is "" which will
      -- register every locally registered process in the Zookeeper services
      -- node.
      registerPrefix :: String
      -- | An operation that will be called for trace level logging.
      -- 'defaultConfig' uses 'nolog'.
    , logTrace :: String -> Process ()
      -- | An operation that will be called for error logging.
      -- jdefaultConfig' uses 'say'
    , logError :: String -> Process ()
    }

-- | A no-op that can be used for either of the loggers in 'Config'.
-- Because no actual I/O is performed, it fully evaluates the message so
-- thunks do not build up.
nolog :: String -> Process ()
nolog m = m `deepseq` return ()

-- | Simple formatter for trace output through 'say'.
sayTrace :: String -> Process ()
sayTrace = say . ("[C.D.P.Zookeeper: TRACE] - " ++)

-- | By default all local names are registered with zookeeper, and only
-- error messages are logged through 'say'.
defaultConfig :: Config
defaultConfig = Config "" nolog (say . ("[C.D.P.Zookeeper: ERROR] - " ++))

-- |Register a name and pid as a service in Zookeeper. The controller
-- will monitor the pid and remove its child node from Zookeeper when it
-- exits.
--
-- Names will be registered at "/distributed-process/services/<name>/<pid>"
--
-- Note: By default all locally registered names (using 'register') will be
-- registered in Zookeeper under the same name by an MxAgent process. Use
-- this function if you want to register an anonymous pid or using
-- a different name - or if you are using a prefix to exclude the automatic
-- registration (see 'Config').
registerZK :: String -> ProcessId -> Process (Either String ())
registerZK name rpid = callZK $ Register name rpid

-- | Get a list of nodes advertised in Zookeeper. These are registered
-- when 'zkController' starts in path
-- "/distributed-process/controllers/<pid>".
--
-- Note: this is included for API compatibility with P2P but its usage
-- would suggest discovery patterns that could be made more efficient
-- when using Zookeeper - e.g. just use (automatic) advertisement and getCapable.
getPeers :: Process [NodeId]
getPeers = fmap processNodeId <$> callZK (GetRegistered controllersNode)

-- | Returns list of pids registered with the service name.
--
-- Results are cached by the controller until they are invalidated by
-- subsequent changes to the service node in Zookeeper, which is
-- communicated through a 'Watcher'. Data will be fetched from Zookeeper
-- only when it is changed and then requested again.
getCapable :: String -> Process [ProcessId]
getCapable name = callZK (GetRegistered (servicesNode </> name))

-- | Broadcast a message to a specific service on all registered nodes.
--
-- Note: this is included for API compatibility with P2P but its usage
-- would suggest discovery patterns that could be made more efficient
-- when using Zookeeper - e.g. just use getCapable to
-- send a broadcast directly to the registered process on each node.
nsendPeers :: Serializable a => String -> a -> Process ()
nsendPeers service msg = getPeers >>= mapM_ (\peer -> nsendRemote peer service msg)

-- | Broadcast a message to a all pids registered with a particular service
-- name.
nsendCapable :: Serializable a => String -> a -> Process ()
nsendCapable service msg = getCapable service >>= mapM_ (`send` msg)

-- | Run a Zookeeper service process, and installs an MXAgent to
-- automatically register all local names in Zookeeper using default
-- options.
zkController :: String -- ^ The Zookeeper endpoint(s) -- comma separated list of host:port
             -> Process ()
zkController = zkControllerWith defaultConfig

-- | As 'zkController' but accept 'Config' options rather than assuming
-- defaults.
zkControllerWith :: Config
                 -> String -- ^ The Zookeeper endpoint(s) -- comma separated list of host:port
                 -> Process ()
zkControllerWith config keepers =
 do run <- spawnProxy
    liftIO $ ZK.withZookeeper keepers 1000 (Just inits) Nothing $ \rzh ->
        run (server rzh config)
  where
    inits rzh SessionEvent ZK.ConnectedState _ =
      void $
       do create rzh rootNode Nothing OpenAclUnsafe [] >>= assertNode rootNode
          create rzh servicesNode Nothing OpenAclUnsafe [] >>= assertNode servicesNode
          create rzh controllersNode Nothing OpenAclUnsafe [] >>= assertNode controllersNode

    inits _ _ _ _ = return ()

server :: Zookeeper -> Config -> Process ()
server rzh config@Config{..} =
 do pid <- getSelfPid
    register controller pid
    watchRegistration config
    proxy' <- spawnProxy
    regself <- create rzh (controllersNode </> pretty pid)
                          (pidB pid) OpenAclUnsafe [Ephemeral]
    case regself of
        Left reason ->
            let msg = "Could not register self with Zookeeper: " ++ show reason
            in logError msg >> liftIO (throwIO (userError msg))
        Right _ -> return ()
    let loop st =
          let recvCmd = match $ \command -> case command of
                                  Exit -> return ()
                                  _ -> handle st command >>= loop
              recvMon = match $ \(ProcessMonitorNotification _ dead _) ->
                                  reap st dead >>= loop
          in logTrace (show st) >> receiveWait [recvCmd, recvMon]
    void $ loop (State mempty mempty pid proxy')
  where
    watchCache State{..} _ _ ZK.ConnectedState (Just node) =
        proxy $ send spid (ClearCache node)

    watchCache _ _ _ _ _ = return ()

    reap st@State{..} pid =
     do let names = fromMaybe [] (Map.lookup pid monPids)
        forM_ names $ \name ->
         do let node = servicesNode </> name </> pretty pid
            result <- delete rzh node Nothing
            case result of
                  Left reason -> logError  $ show reason
                                          ++ " - failed to delete "
                                          ++ node
                  _ -> logTrace $ "Reaped " ++ node
        return st{monPids = Map.delete pid monPids}

    handle st@State{..} (Register name rpid reply) =
     do let node = servicesNode </> name </> pretty rpid
        create rzh (servicesNode </> name)
                    Nothing OpenAclUnsafe [] >>= assertNode name
        result <- create rzh node (pidB rpid)
                         OpenAclUnsafe [Ephemeral]
        case result of
            Right _ ->
             do logTrace $ "Registered " ++ node
                sendChan reply (Right ())
            Left reason ->
             do logError $ "Failed to register name: " ++ node ++ " - " ++ show reason
                sendChan reply (Left $ show reason)

        void $ monitor rpid
        return st{monPids = Map.insertWith (++) rpid [name] monPids}

    handle st@State{..} (ClearCache node) =
        return st{nodeCache = Map.delete node nodeCache}

    handle st@State{..} (GetRegistered node reply) =
     do epids <- case Map.lookup node nodeCache of
                        Just pids -> return (Right pids)
                        Nothing -> getChildPids rzh node (Just $ watchCache st)
        case epids of
            Right pids ->
             do sendChan reply pids
                return st{nodeCache = Map.insert node pids nodeCache}
            Left reason ->
             do logError $  "Retrieval failed for node: " ++ node ++ " - " ++ reason
                sendChan reply []
                return st{nodeCache = Map.delete node nodeCache}

    handle st Exit = return st --satisfy exhaustiveness checker

    pretty pid = drop 6 (show pid)
    pidB pid = Just . BL.toStrict $ encode pid

watchRegistration :: Config -> Process ()
watchRegistration Config{..} = do
    let initState = [] :: [MxEvent]
    void $ mxAgent (MxAgentId "zookeeper-name-listener") initState [
        mxSink $ \ev -> do
           let act =
                 case ev of
                   (MxRegistered _ "zookeeper-name-listener") -> return ()
                   (MxRegistered pid name')
                        | prefixed name' -> liftMX $
                                do ereg <- registerZK name' pid
                                   case ereg of
                                       Left reason ->
                                          logError $ "Automatic registration failed for name: "
                                                ++ name' ++ " - " ++ reason
                                       _ -> return ()
                        | otherwise -> return ()
                   _                   -> return ()
           act >> mxReady ]
    liftIO $ threadDelay 10000
  where prefixed = isPrefixOf registerPrefix

getChildPids :: MonadIO m
             => Zookeeper -> String -> Maybe Watcher
             -> m (Either String [ProcessId])
getChildPids rzh node watcher = liftIO $
 do enodes' <- ZK.getChildren rzh node watcher
    case enodes' of
        Left NoNodeError -> return $ Right []
        _ ->
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

-- | Wait for zkController to startup and register iteself.
waitController :: Process ()
waitController =
 do res <- whereis controller
    case res of
        Nothing -> liftIO (threadDelay 10000) >> waitController
        Just _ -> return ()

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
bootstrap = bootstrapWith defaultConfig --{logTrace = sayTrace}

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
