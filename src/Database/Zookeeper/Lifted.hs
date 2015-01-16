module Database.Zookeeper.Lifted
    ( module Database.Zookeeper
    , withZookeeper
    , create
    , delete
    ) where

import Database.Zookeeper hiding (create, delete, withZookeeper)
import qualified Database.Zookeeper as Base
import Control.Monad.Trans.Control (control)
import Control.Monad.Base (liftBase)

withZookeeper s t w c zma = control $ \runInIO ->
    Base.withZookeeper s t w c (runInIO . zma)

create z n d a = liftBase . Base.create z n d a

delete z n = liftBase . Base.delete z n
