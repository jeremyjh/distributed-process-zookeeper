{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}

module Control.Distributed.Process.Zookeeper
   ( zkController
   , registerZK
   ) where

import Control.Distributed.Process hiding (proxy)
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.MonadBaseControl ()
import Database.Zookeeper.Lifted
import Control.Monad (void, join)
import Control.Concurrent.MVar (newEmptyMVar, takeMVar, putMVar)
import Control.Exception (throwIO)
import Data.Binary (Binary, encode)
import Data.Typeable (Typeable)
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as BS
import GHC.Generics (Generic)

data Command = Register String ProcessId (SendPort (Either String ()))
             | NodeList [String]
             | Exit
    deriving (Show, Typeable, Generic)

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
            let loop st@(_, nodes) =
                 do liftIO $ print ("nodes", nodes)
                    command <- expect
                    case command of
                        Exit -> return ()
                        _ -> handle st command >>= loop
            in loop (rzh, [])
    register controller zkpid
  where
    handle st@(rzh, _) (Register name rpid reply) =
     do result <- create rzh (rootNode </> name) (pidB rpid) OpenAclUnsafe [Ephemeral]
        case result of
            Right _ -> sendChan reply (Right ())
            Left reason -> sendChan reply (Left $ show reason)
        return st

    handle (rzh, _) (NodeList nodes) =
        return (rzh, nodes)

    handle st Exit = return st

    watcher proxy pid rzh SessionEvent ConnectedState _ =
      void $ do
                create rzh rootNode Nothing OpenAclUnsafe [] >>= assertNode rootNode
                create rzh servicesNode Nothing OpenAclUnsafe [] >>= assertNode servicesNode
                create rzh controllersNode Nothing OpenAclUnsafe [] >>= assertNode controllersNode
                Right _ <- create rzh (controllersNode </> pretty pid)
                                      (pidB pid) OpenAclUnsafe [Ephemeral]
                Right children <- getChildren rzh controllersNode Nothing --(rootP </> "nodes") Nothing
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
    assertNode name (Left _) = throwIO (userError $ "Fatal: could not create node: " ++ name)

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
