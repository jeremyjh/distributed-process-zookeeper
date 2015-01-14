{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}

module Control.Distributed.Process.Zookeeper
   ( zkController
   , registerZK
   ) where

import Control.Distributed.Process
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.MonadBaseControl ()
import Database.Zookeeper.Lifted
import Control.Monad (void)
import Data.Binary (Binary, encode)
import Data.Typeable (Typeable)
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as BS
import GHC.Generics (Generic)

data Command = Register String ProcessId (SendPort (Either String ()))
             | Exit
    deriving (Show, Typeable, Generic)

instance Binary Command

-- | Starts a Zookeeper service process, and installs an MXAgent to
-- automatically register all local names in Zookeeper.
zkController :: String -- ^ The Zookeeper endpoint, as in 'Base.withZookeeper'
             -> Process ()
zkController services =
   void $ spawnLocal $
     do pid <- getSelfPid
        withZookeeper services 1000 (Just $ watcher pid) Nothing $ \rzh ->
            let loop =
                 do command <- expect
                    case command of
                        Exit -> return ()
                        _ -> handle rzh command >> loop
            in loop
  where
    handle rzh (Register name rpid reply) =
     do result <- create rzh (name </> pidS rpid) Nothing OpenAclUnsafe [Ephemeral]
        case result of
            Right _ -> sendChan reply (Right ())
            Left reason -> sendChan reply (Left $ show reason)

    handle _ Exit = return ()

    watcher pid rzh SessionEvent ConnectedState _ =
      void $ do void $ create rzh (pidS pid) Nothing OpenAclUnsafe [Ephemeral]
                Right children <- getChildren rzh "/" Nothing
                mapM putStrLn children

    watcher _ _ _ _ _ = return ()

    serviceP name = rootP </> "services" </> name
    rootP = "/distributed-process"
    l </> r = l ++ "/" ++ r
    pidS pid = "/" ++ drop 6 (show pid)
    pidB pid = Just . BS.concat . BL.toChunks $ encode pid

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
