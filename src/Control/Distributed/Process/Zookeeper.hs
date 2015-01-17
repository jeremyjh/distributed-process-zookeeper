{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE RecordWildCards    #-}

module Control.Distributed.Process.Zookeeper
   ( zkController
   , registerZK
   , getPeers
   ) where

import           Database.Zookeeper                       (AclList (..),
                                                           CreateFlag (..),
                                                           Event (..),
                                                           ZKError (..),
                                                           Zookeeper)
import qualified Database.Zookeeper                       as ZK

import           Control.Concurrent                       (forkIO)
import           Control.Concurrent.MVar                  (newEmptyMVar,
                                                           putMVar, takeMVar)
import           Control.Exception                        (throwIO)
import           Control.Monad                            (forM, forM_, join,
                                                           void)
import           Control.Monad.Except                     (ExceptT (..), lift)
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
import           Control.Distributed.Process.Node         (LocalNode,
                                                           runProcess)
import           Control.Distributed.Process.Serializable



data Command = Register String ProcessId (SendPort (Either String ()))
             | NodeList [String]
             | GetControllerNodeIds (SendPort (Either String [NodeId]))
             | Exit
    deriving (Show, Typeable, Generic)

data State = State
    {
      conn    :: Zookeeper
    , nodes   :: [String]
    -- service nodes to remove each pid from when it exits
    , monPids :: Map ProcessId [String]
    }
instance Show State where
    show State{..} = show ("nodes", nodes, "monPids", monPids)

instance Binary Command

-- | Starts a Zookeeper service process, and installs an MXAgent to
-- automatically register all local names in Zookeeper.
zkController :: LocalNode
             -> String -- ^ The Zookeeper endpoint, as in 'Base.withZookeeper'
             -> Process ()
zkController localnode services =
 do proxy <- spawnProxy
    liftIO $ void $ forkIO $
        ZK.withZookeeper services 1000 (Just $ watcher proxy) Nothing $ \rzh ->
            runProcess localnode $
             do pid <- getSelfPid
                register controller pid
                Right _ <- create rzh (controllersNode </> pretty pid)
                                      (pidB pid) OpenAclUnsafe [Ephemeral]
                Right nodes' <- liftIO $ ZK.getChildren rzh controllersNode Nothing

                let loop st@State{..} =
                        let recvCmd = match $ \command -> case command of
                                                Exit -> terminate --TODO: this could be better
                                                _ -> handle st command
                            recvMon = match $ \(ProcessMonitorNotification _ dead _) ->
                                                reap st dead
                        in say (show st) >> receiveWait [recvCmd, recvMon] >>= loop
                loop (State rzh nodes' mempty)
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

    handle st@State{..} (GetControllerNodeIds reply) =
     do enodes' <- liftIO $ ZK.getChildren conn controllersNode Nothing
        eall <- runExceptT $
         do nodes' <- hoistEither (zkeither enodes')
            forM nodes' $ \node' ->
             do eresult <- liftIO $ ZK.get conn node' Nothing
                case eresult of
                   Left reason ->
                     do let msg = "Error fetching data for controller: " ++ node'
                                  ++ " : " ++ show reason
                        lift $ say msg
                        throwE msg
                   Right (Nothing, _) ->
                     do let msg = "Error fetching data for controller: " ++ node'
                                  ++ " : data was empty."
                        lift $ say msg
                        throwE msg
                   Right (Just bs, _) ->
                       let pid = decode (BL.fromStrict bs) in return (processNodeId pid)
        case eall of
            Left reason -> sendChan reply (Left reason)
            Right results -> sendChan reply (Right results)
        return st

    handle st (NodeList n) = return st{nodes = n}

    handle st Exit = return st

    watcher _ rzh SessionEvent ZK.ConnectedState _ =
      void $ do
                create rzh rootNode Nothing OpenAclUnsafe [] >>= assertNode rootNode
                create rzh servicesNode Nothing OpenAclUnsafe [] >>= assertNode servicesNode
                create rzh controllersNode Nothing OpenAclUnsafe [] >>= assertNode controllersNode

    watcher _ _ _ _ _ = return ()

    servicesNode = rootNode </> "services"
    controllersNode = rootNode </> "controllers"
    rootNode = "/distributed-process"
    l </> r = l ++ "/" ++ r
    pretty pid = drop 6 (show pid)
    pidB pid = Just . BL.toStrict $ encode pid

    zkeither (Left zkerr) = Left $ show zkerr
    zkeither (Right a) = Right a
    hoistEither = ExceptT . return

    assertNode _ (Right _) = return ()
    assertNode _ (Left NodeExistsError) = return ()
    assertNode name (Left _) = liftIO $
        throwIO (userError $ "Fatal: could not create node: " ++ name)

registerZK :: String -> ProcessId -> Process (Either String ())
registerZK name rpid =
    callZK $ Register name rpid

-- | Get a list of currently available peer nodes.
getPeers :: Process [NodeId]
getPeers = either (const []) id `fmap` callZK GetControllerNodeIds

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

spawnProxy :: Process (Process () -> IO ())
spawnProxy =
 do mv <- liftIO newEmptyMVar
    void $ spawnLocal $
        let loop = join (liftIO $ takeMVar mv) >> loop
        in loop
    return (putMVar mv)

create :: MonadIO m => Zookeeper -> String -> Maybe BS.ByteString
       -> AclList -> [CreateFlag] -> m (Either ZKError String)
create z n d a = liftIO . ZK.create z n d a

delete :: MonadIO m => Zookeeper -> String -> Maybe ZK.Version -> m (Either ZKError ())
delete z n = liftIO . ZK.delete z n
