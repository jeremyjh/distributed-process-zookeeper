{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE RecordWildCards    #-}

module Control.Distributed.Process.Zookeeper
   (
   -- * Startup
     bootstrap
   , bootstrapWith
   , zkController
   , zkControllerWith
   -- * Basic API
   , registerZK
   , getPeers
   , getCapable
   , nsendPeers
   , nsendCapable
   -- * Globals
   , registerCandidate
   , whereisGlobal
   -- * Config
   , Config(..)
   , defaultConfig
   -- * Utility
   , nolog
   , sayTrace
   , waitController
   ) where

import           Database.Zookeeper                       (AclList (..),
                                                           CreateFlag (..),
                                                           Event (..), Watcher,
                                                           ZKError (..),
                                                           Zookeeper)
import qualified Database.Zookeeper                       as ZK

import           Control.Applicative                      ((<$>))
import           Control.Concurrent                       (myThreadId,
                                                           newEmptyMVar,
                                                           putMVar, putMVar,
                                                           takeMVar, takeMVar,
                                                           threadDelay)
import           Control.DeepSeq                          (deepseq)
import           Control.Exception                        (bracket, throwTo)
import           Control.Monad                            (forM, join, void)
import           Control.Monad.Except                     (ExceptT (..), lift)
import           Control.Monad.IO.Class                   (MonadIO)
import           Control.Monad.Trans.Except               (runExceptT, throwE)
import           Data.Binary                              (Binary, decode,
                                                           encode)
import           Data.ByteString                          (ByteString)
import qualified Data.ByteString                          as BS
import qualified Data.ByteString.Lazy                     as BL
import           Data.Foldable                            (forM_)
import           Data.List                                (isPrefixOf, sort)
import           Data.Map.Strict                          (Map)
import qualified Data.Map.Strict                          as Map
import           Data.Maybe                               (fromMaybe)
import           Data.Monoid                              (mempty)
import           Data.Typeable                            (Typeable)
import           GHC.Generics                             (Generic)

import           Control.Distributed.Process              hiding (bracket,
                                                           proxy)
import           Control.Distributed.Process.Management
import           Control.Distributed.Process.Node         (newLocalNode,
                                                           runProcess)
import           Control.Distributed.Process.Serializable
import           Network                                  (HostName)
import           Network.Socket                           (ServiceName)
import           Network.Transport                        (closeTransport)
import           Network.Transport.TCP                    (createTransport,
                                                           defaultTCPParameters)


data Command = Register String ProcessId (SendPort (Either String ()))
             | GlobalCandidate String ProcessId (SendPort (Either String ProcessId))
             | CheckCandidate String
             | ClearCache String
             | GetRegistered String (SendPort [ProcessId])
             | GetGlobal String (SendPort (Maybe ProcessId))
             | Exit
    deriving (Show, Typeable, Generic)

instance Binary Command

data Elect = Elect deriving (Typeable, Generic)
instance Binary Elect

data State = State
    {
      nodeCache  :: Map String [ProcessId]
    -- service nodes to remove each pid from when it exits
    , monPids    :: Map ProcessId ([String],[String])
    , candidates :: Map String (String, ProcessId)
    , spid       :: ProcessId
    , proxy      :: Process () -> IO ()
    , conn       :: Zookeeper
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
    , logTrace       :: String -> Process ()
      -- | An operation that will be called for error logging.
      -- 'defaultConfig' uses 'say'
    , logError       :: String -> Process ()
      -- | The log level for the C Zookeper library. 'defaultConfig' uses
      -- 'ZK.ZLogWarn'.
    , zLogLevel      :: ZK.ZLogLevel
      -- | The ACL to use for every node - see hzk documentation for
      -- 'ZK.AclList'. Note that if your nodes do not
      -- connect with the same identity, every node will need at least Read
      -- permission to all nodes created by this package.
    , acl            :: ZK.AclList
      -- | Credentials for Zookeeper, see hzk 'ZK.addAuth' for details.
    , credentials    :: Maybe (ZK.Scheme, ByteString)
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
--
-- > defaultConfig = Config {
-- >       registerPrefix = ""
-- >     , logTrace = nolog
-- >     , logError = say . ("[C.D.P.Zookeeper: ERROR] - " ++)
-- >     , zLogLevel = ZK.ZLogWarn
-- >     , acl = OpenAclUnsafe
-- >     , credentials = Nothing
-- >     }
defaultConfig :: Config
defaultConfig = Config {
      registerPrefix = ""
    , logTrace = nolog
    , logError = say . ("[C.D.P.Zookeeper: ERROR] - " ++)
    , zLogLevel = ZK.ZLogWarn
    , acl = OpenAclUnsafe
    , credentials = Nothing
    }

-- |Register a name and pid as a service in Zookeeper. The controller
-- will monitor the pid and remove its child node from Zookeeper when it
-- exits.
--
-- Names will be registered at "\/distributed-process\/services\/\<name\>\/\<pid\>"
--
-- Note: By default all locally registered names (using 'register') will be
-- registered in Zookeeper under the same name by an MxAgent process. Use
-- this function if you want to register an anonymous pid or use
-- a different name than is registered with the local Process, or when
-- you are using a prefix to exclude the automatic
-- registration (see 'Config').
registerZK :: String -> ProcessId -> Process (Either String ())
registerZK name rpid = callZK $ Register name rpid

-- | Get a list of nodes advertised in Zookeeper. These are registered
-- when 'zkController' starts in path
-- "\/distributed-process\/controllers\/\<pid\>".
--
-- Note: this is included for API compatibility with P2P but its usage
-- would suggest discovery patterns that could be made more efficient
-- when using Zookeeper - i.e. just use 'getCapable'.
getPeers :: Process [NodeId]
getPeers = fmap processNodeId <$> callZK (GetRegistered controllersNode)

-- | Returns list of pids registered with the service name.
--
-- Results are cached by the controller until they are invalidated by
-- subsequent changes to the service node in Zookeeper, which is
-- communicated through a 'Watcher'. Data will be fetched from Zookeeper
-- only when it is changed and then requested again.
getCapable :: String -> Process [ProcessId]
getCapable name =
    callZK (GetRegistered (servicesNode </> name))


-- | Broadcast a message to a specific service on all registered nodes.
--
-- Note: this is included for API compatibility with P2P but its usage
-- would suggest discovery patterns that could be made more efficient
-- when using Zookeeper - i.e. just use 'sendCapable' to
-- send a broadcast directly to the registered process on each node.
nsendPeers :: Serializable a => String -> a -> Process ()
nsendPeers service msg = getPeers >>= mapM_ (\peer -> nsendRemote peer service msg)

-- | Broadcast a message to a all pids registered with a particular service
-- name.
nsendCapable :: Serializable a => String -> a -> Process ()
nsendCapable service msg = getCapable service >>= mapM_ (`send` msg)

-- | Register a candidate process for election of a single process
-- associated to the given global name, and returns the process ID of the
-- elected global (which may or may not be on the local node). The @Process ()@ argument is
-- only evaluated if this node ends up being the elected host for the
-- global. Calling this function subsequently on the same node for the same
-- name will replace the current candidate computation with the new one.
registerCandidate :: String -> Process () -> Process (Either String ProcessId)
registerCandidate name proc = stage >>= callZK . GlobalCandidate name
  where stage = spawnLocal $ (expect :: Process Elect) >> proc

-- | Find a registered global by name - see 'registerCandidate'.
whereisGlobal :: String -> Process (Maybe ProcessId)
whereisGlobal = callZK . GetGlobal

-- | Run a Zookeeper service process, and installs an MXAgent to
-- automatically register all local names in Zookeeper using default
-- options.
--
-- > zkController = zkControllerWith defaultConfig
zkController :: String -- ^ The Zookeeper endpoint(s) -- comma separated list of host:port
             -> Process ()
zkController = zkControllerWith defaultConfig

-- | As 'zkController' but accept 'Config' options rather than assuming
-- defaults.
zkControllerWith :: Config
                 -> String -- ^ The Zookeeper endpoint(s) -- comma separated list of host:port
                 -> Process ()
zkControllerWith config@Config{..} keepers =
 do run <- spawnLinkedProxy
    waitInit <- liftIO newEmptyMVar
    liftIO $ ZK.setDebugLevel zLogLevel
    mthread <- liftIO myThreadId
    liftIO $ ZK.withZookeeper keepers 1000 (Just $ inits mthread waitInit) Nothing $ \rzh ->
     do init' <- takeMVar waitInit
        case init' of
            Left reason -> run $ do logError $ "Init failed: " ++ show reason
                                    die reason
            Right ()    -> run (server rzh config)
  where
    inits _ waitInit rzh SessionEvent ZK.ConnectedState _ =
     do esetup <- runExceptT $ eauth >> dosetup
        case esetup of
            Right _ -> putMVar waitInit (Right ())
            Left reason -> putMVar waitInit (Left reason)
        where
          eauth = lift auth >>= hoistEither
          auth =
             case credentials of
                  Nothing -> return (Right ())
                  Just (scheme, creds) ->
                   do waitAuth <- newEmptyMVar
                      ZK.addAuth rzh scheme creds $ \ res ->
                         case res of
                             Left reason ->
                                 let msg = "Authentication to Zookeeper failed: " ++ show reason
                                 in putMVar waitAuth (Left msg)
                             Right () -> putMVar waitAuth (Right ())
                      takeMVar waitAuth
          dosetup =
           do esetup <- runExceptT $
               do createAssert rzh "/" Nothing acl []
                  createAssert rzh rootNode Nothing acl []
                  createAssert rzh servicesNode Nothing acl []
                  createAssert rzh controllersNode Nothing acl []
                  createAssert rzh globalsNode Nothing acl []
                  createAssert rzh (globalsNode </> "blarg") Nothing acl []
              case esetup of
                  Right _ -> return ()
                  Left reason ->
                      throwE $ "FATAL: could not create system nodes in Zookeeper: "
                             ++ show reason

    inits zkthread _ _ SessionEvent ZK.ExpiredSessionState _ =
        --for some reason having a linked proxy die here was not getting it done
        throwTo zkthread $ userError "Zookeeper session expired."

    inits _ _ _ _ _ _ = return ()

server :: Zookeeper -> Config -> Process ()
server rzh config@Config{..} =
 do pid <- getSelfPid
    register controller pid
    watchRegistration config
    proxy' <- spawnLinkedProxy
    regself <- create rzh (controllersNode </> pretty pid)
                          (pidBytes pid) acl [Ephemeral]
    case regself of
        Left reason ->
            let msg = "Could not register self with Zookeeper: " ++ show reason
            in logError msg >> die msg
        Right _ -> return ()
    let loop st =
          let recvCmd = match $ \command -> case command of
                                  Exit -> return ()
                                  _ ->
                                    do eresult <- runExceptT $ handle st command
                                       case eresult of
                                           Right st' ->
                                              do logTrace $ "State of: " ++ show st' ++ " - after - "
                                                           ++ show command
                                                 loop st'
                                           Left reason ->
                                              do logError $ "Error handling: " ++ show command
                                                            ++ " : " ++ show reason
                                                 loop st
              recvMon = match $ \(ProcessMonitorNotification _ dead _) ->
                                  reap st dead >>= loop
          in logTrace (show st) >> receiveWait [recvCmd, recvMon]
    void $ loop (State mempty mempty mempty pid proxy' rzh)
  where
    reap st@State{..} pid =
     do let (services, globals) = fromMaybe ([],[]) (Map.lookup pid monPids)
        forM_ services $ \name ->
            deleteNode (servicesNode </> name </> pretty pid)
        forM_ globals $ \name ->
            deleteNode (globalsNode </> name)

        return st{monPids = Map.delete pid monPids}
      where
        deleteNode node =
         do result <- liftIO $ ZK.delete rzh node Nothing
            case result of
                  Left reason -> logError  $ show reason
                                          ++ " - failed to delete "
                                          ++ node
                  _ -> logTrace $ "Reaped " ++ node

    handle st@State{..} (Register name rpid reply) =
     do let node = servicesNode </> name </> pretty rpid
        createAssert rzh (servicesNode </> name) Nothing acl []
        result <- create rzh node (pidBytes rpid) acl [Ephemeral]
        case result of
            Right _ -> lift $
             do logTrace $ "Registered " ++ node
                sendChan reply (Right ())
                void $ monitor rpid
                let apService (a',b') (a, b) = (a' ++ a, b' ++ b)
                return st{monPids = Map.insertWith apService rpid ([name],[]) monPids}
            Left reason -> lift $
             do logError $ "Failed to register name: " ++ node ++ " - " ++ show reason
                sendChan reply (Left $ show reason)
                return st



    handle st@State{..} (ClearCache node) =
        return st{nodeCache = Map.delete node nodeCache}

    handle st@State{..} (GetRegistered node reply) =
     do epids <- case Map.lookup node nodeCache of
                        Just pids -> return (Right pids)
                        Nothing -> getChildPids rzh node (Just $ watchCache st node)
        lift $ case epids of
            Right pids ->
             do sendChan reply pids
                return st{nodeCache = Map.insert node pids nodeCache}
            Left reason ->
             do logError $  "Retrieval failed for node: " ++ node ++ " - " ++ show reason
                sendChan reply []
                return st{nodeCache = Map.delete node nodeCache}

    handle st (GlobalCandidate n c r) = handleGlobalCandidate config st n c r

    handle st@State{..} (GetGlobal name reply) =
        let gname = globalsNode </> name in
        case Map.lookup gname nodeCache of
            Just (pid : _) -> lift $
               do sendChan reply (Just pid)
                  return st
            _              ->
               do elected <- getElected
                  lift $ sendChan reply elected
                  return $ maybe st
                                 (\pid -> st {nodeCache = Map.insert gname [pid] nodeCache})
                                 elected
      where
        getElected =
         do children <- getGlobalIds conn name
            case children of
                [] -> return Nothing
                (first: _) ->
                    Just <$> getPid conn (globalsNode </> name </> first)
                                         (Just $ watchCache st name)

    handle st@State{..} (CheckCandidate name) =
        case Map.lookup name candidates of
            Just (myid, staged) -> snd <$> mayElect st name myid staged
            Nothing             -> return st

    handle st Exit = return st --satisfy exhaustiveness checker

    pretty pid = drop 6 (show pid)

handleGlobalCandidate :: Config -> State
                      -> String -> ProcessId -> SendPort (Either String ProcessId)
                      -> ExceptT ZKError Process State
handleGlobalCandidate Config{..} st@State{..} name proc reply
    | Just (myid, staged) <- Map.lookup name candidates =
          case Map.lookup (globalsNode </> name) nodeCache of
              Just (pid : _) -> lift $
                                 do exit staged "New candidate staged."
                                    sendChan reply (Right pid)
                                    return st {candidates = Map.insert name (myid, proc) candidates}

              _              -> respondElect myid proc (Just staged)
    | otherwise =
         do myid <- registerGlobalId proc
            respondElect myid proc Nothing
      where
        respondElect myid staged mprev = lift $
         do eresult <- runExceptT $ mayElect st name myid staged
            case eresult of
                Right (pid, st') ->
                 do sendChan reply (Right pid)
                    forM_ mprev (`exit` "New candidate staged.")
                    return st'
                Left reason ->
                 do sendChan reply (Left $ show reason)
                    return st

        registerGlobalId staged =
         do let pname = globalsNode </> name
            createAssert conn pname Nothing acl []
            node <- create conn (pname </> "")
                                (pidBytes staged)
                                acl
                                [Ephemeral, Sequence] >>= hoistEither
            return $ extractId node
          where
            extractId s = trimId (reverse s) ""
              where
                trimId [] _ = error $ "end of string without delimiter / in " ++ s
                trimId ('/' : _) str = str
                trimId (n : rest) str = trimId rest (n : str)

mayElect :: State -> String -> String -> ProcessId -> ExceptT ZKError Process (ProcessId, State)
mayElect st@State{..} name myid staged =
 do others <- getGlobalIds conn name
    let first : _ = others
    if myid == first
        then
         do lift $ send staged Elect
            st' <- lift $ cacheNMonitor staged
            return (staged, st')
        else
         do let prev = findPrev others
            watchFirst <- if prev == first then return $ Just watchPid
                          else do void $ liftIO (ZK.exists
                                                    conn
                                                    (globalsNode </> name </> prev)
                                                    (Just watchPid)) >>= hoistEither
                                  return Nothing
            pid <- getPid conn (globalsNode </> name </> first) watchFirst
            return (pid, stCache)
  where
    findPrev (prev : next : rest) = if next == myid
                                       then prev
                                       else findPrev (next : rest)
    findPrev _ = error "impossible: couldn't find myself in election"
    watchPid _ ZK.DeletedEvent ZK.ConnectedState _ =
        proxy $ send spid (CheckCandidate name)
    watchPid _ _ _ _ = return ()

    stCandidate = st{candidates = Map.insert name (myid, staged) candidates}
    stCache = stCandidate {nodeCache = Map.insert (globalsNode </> name) [staged] nodeCache}
    cacheNMonitor pid =
     do void $ monitor pid
        liftIO $ void $ ZK.exists conn (globalsNode </> name </> myid) (Just $ watchCache st (globalsNode </> name))
        return stCacheMon
      where
        stCacheMon =
             let apGlobal (a', b') (a, b) = (a' ++ a, b' ++ b) in
             st{ candidates = Map.delete name candidates
               , monPids = Map.insertWith apGlobal
                                          pid
                                          ([],[name </> myid]) monPids
               , nodeCache = Map.insert (globalsNode </> name) [pid] nodeCache
               }

getGlobalIds :: MonadIO m
             => Zookeeper -> String -> ExceptT ZKError m [String]
getGlobalIds conn name = liftIO $
 do echildren <- ZK.getChildren conn (globalsNode </> name) Nothing
    case echildren of
        Left NoNodeError -> return []
        Left reason ->
         do putStrLn $ "Could not fetch globals for " ++ name ++ " - " ++ show reason
            return []
        Right children -> return (sort children)

getPid :: MonadIO m
       => Zookeeper -> String -> Maybe Watcher
       -> ExceptT ZKError m ProcessId
getPid conn name watcher =
 do res <- liftIO $ ZK.get conn name watcher
    case res of
        Right (Just bs, _) -> return (decode $ BL.fromStrict bs)
        Right _ -> throwE NothingError
        Left reason -> throwE reason

getChildPids :: MonadIO m
             => Zookeeper -> String -> Maybe Watcher
             -> m (Either ZKError [ProcessId])
getChildPids rzh node watcher = liftIO $
 do enodes' <- ZK.getChildren rzh node watcher
    case enodes' of
        Left NoNodeError -> return $ Right []
        _ ->
            runExceptT $
             do children <- hoistEither enodes'
                forM children $ \child ->
                 do eresult <- liftIO $ ZK.get rzh (node </> child) Nothing
                    case eresult of
                       Left reason        -> throwE reason
                       Right (Nothing, _) -> throwE NothingError
                       Right (Just bs, _) -> return (decode $ BL.fromStrict bs)

watchCache :: State -> String -> a -> b -> ZK.State -> c -> IO ()
watchCache State{..} node _ _ ZK.ConnectedState _ =
    proxy $ send spid (ClearCache node)

watchCache _ _ _ _ _ _ = return ()

watchRegistration :: Config -> Process ()
watchRegistration Config{..} = do
    let initState = [] :: [MxEvent]
    void $ mxAgent (MxAgentId "zookeeper:name:listener") initState [
        mxSink $ \ev -> do
           let act =
                 case ev of
                   (MxRegistered _ "zookeeper:name:listener") -> return ()
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

-- | Wait for zkController to startup and register iteself.
-- This is only useful if you are *not* using a 'bootstrap'
-- function to start your node, but rather starting the node yourself
-- and using one of the 'zkController' functions..
waitController :: Int -> Process (Maybe ())
waitController timeout =
 do res <- whereis controller
    case res of
        Nothing ->
            do let timeleft = timeout - 10000
               if timeleft <= 0 then return Nothing
                  else do liftIO (threadDelay 10000)
                          waitController timeleft
        Just _ -> return (Just ())

stopController :: Process ()
stopController =
 do res <- whereis controller
    case res of
        Nothing -> say "Could not find controller to stop it."
        Just pid -> send pid Exit

hoistEither :: Monad m => Either e a -> ExceptT e m a
hoistEither = ExceptT . return

(</>) :: String -> String -> String
l </> r = l ++ "/" ++ r

servicesNode :: String
servicesNode = rootNode </> "services"

controllersNode :: String
controllersNode = rootNode </> "controllers"

globalsNode :: String
globalsNode = rootNode </> "globals"

rootNode :: String
rootNode = "/distributed-process"

createAssert :: MonadIO m
             => Zookeeper -> String -> Maybe BS.ByteString -> AclList -> [CreateFlag]
             -> ExceptT ZKError m ()
createAssert z n d a f = create z n d a f >>= eitherExists
  where
    eitherExists (Right _)  = return ()
    eitherExists (Left NodeExistsError) = return ()
    eitherExists (Left reason) = throwE reason

pidBytes :: ProcessId -> Maybe BS.ByteString
pidBytes = Just . BL.toStrict . encode

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
--
-- This is linked bidirectionally - if either the proxy or parent exits the
-- other will as well.
spawnLinkedProxy :: Process (Process a -> IO a)
spawnLinkedProxy =
 do action <- liftIO newEmptyMVar
    result <- liftIO newEmptyMVar
    self <- getSelfPid
    pid <- spawnLocal $
        let loop = join (liftIO $ takeMVar action)
                   >>= liftIO . putMVar result
                   >> loop in link self >> loop
    link pid
    return (\f -> putMVar action f >> takeMVar result)

create :: MonadIO m => Zookeeper -> String -> Maybe BS.ByteString
       -> AclList -> [CreateFlag] -> m (Either ZKError String)
create z n d a = liftIO . ZK.create z n d a

-- | Create a new Cloud Haskell node on the provided IP/Port; start
-- a Zookeeper-backed controller process with a default configuration
-- connected to the provided Zookeeper server list.
-- Finally execute the supplied Process computation.
--
-- > bootstrap = bootstrapWith defaultConfig
bootstrap :: HostName -- ^ Hostname or IP this Cloud Haskell node will listen on.
          -> ServiceName -- ^ Port or port name this node will listen on.
          -> String -- ^ The Zookeeper endpoint(s) -- comma separated list of host:port
          -> RemoteTable -- ^ Cloud Haskell 'RemoteTable' to use in the new node.
          -> Process () -- ^ Process computation to run in the new node.
          -> IO ()
bootstrap = bootstrapWith defaultConfig

-- | Create a new Cloud Haskell node on the provided IP/Port; start
-- a Zookeeper-backed controller process connected to the provided
-- Zookeeper server list and finally execute the supplied Process computation.
bootstrapWith
          :: Config -- ^ controller configuration
          -> HostName -- ^ Hostname or IP this Cloud Haskell node will listen on.
          -> ServiceName -- ^ Port or port name this node will listen on.
          -> String -- ^ The Zookeeper endpoint(s) -- comma separated list of host:port
          -> RemoteTable -- ^ Cloud Haskell 'RemoteTable' to use in the new node.
          -> Process () -- ^ Process computation to run in the new node.
          -> IO ()
bootstrapWith config host port zservs rtable proc =
    bracket acquire release exec
  where
    acquire =
     do Right tcp <- createTransport host port defaultTCPParameters
        return tcp
    exec tcp =
       do node <- newLocalNode tcp rtable
          runProcess node $
           do zkpid <- spawnLocal $ zkControllerWith config zservs
              link zkpid
              found <- waitController 100000
              case found of
                Nothing -> die "Timeout waiting for Zookeeper controller to start."
                Just () -> proc
              stopController
    release = closeTransport
