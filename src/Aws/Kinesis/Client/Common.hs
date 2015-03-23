-- Copyright (c) 2013-2014 PivotCloud, Inc.
--
-- Aws.Kinesis.Client.Common
--
-- Please feel free to contact us at licensing@pivotmail.com with any
-- contributions, additions, or other feedback; we would love to hear from
-- you.
--
-- Licensed under the Apache License, Version 2.0 (the "License"); you may
-- not use this file except in compliance with the License. You may obtain a
-- copy of the License at http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
-- WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
-- License for the specific language governing permissions and limitations
-- under the License.

{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UnicodeSyntax #-}

-- |
-- Module: Aws.Kinesis.Client.Common
-- Copyright: Copyright © 2013-2014 PivotCloud, Inc.
-- License: Apache-2.0
-- Maintainer: Jon Sterling <jsterling@alephcloud.com>
-- Stability: experimental
--
module Aws.Kinesis.Client.Common
( KinesisKit(..)
, kkConfiguration
, kkKinesisConfiguration
, kkManager
, MonadKinesis
, runKinesis

  -- * Fetching Shards
, streamShardSource
, streamOpenShardSource
, shardIsOpen

  -- * Miscellaneous monad stuff
, mapEnvironment
) where

import qualified Aws
import qualified Aws.Core as Aws
import qualified Aws.Kinesis as Kin
import Control.Exception
import Control.Lens
import Control.Error
import Control.Monad
import Control.Monad.Reader.Class
import Control.Monad.Trans
import Control.Monad.Trans.Reader
import Control.Monad.Trans.Resource
import Control.Monad.Unicode
import Data.Conduit
import qualified Data.Conduit.List as CL
import qualified Network.HTTP.Conduit as HC
import Prelude.Unicode

-- | The 'KinesisKit' contains what is necessary to make a request to Kinesis.
--
data KinesisKit
  = KinesisKit
  { _kkConfiguration ∷ !Aws.Configuration
  , _kkKinesisConfiguration ∷ !(Kin.KinesisConfiguration Aws.NormalQuery)
  , _kkManager ∷ !HC.Manager
  }

-- | A lens for '_kkConfiguration'.
--
kkConfiguration ∷ Lens' KinesisKit Aws.Configuration
kkConfiguration = lens _kkConfiguration $ \kk cfg → kk { _kkConfiguration = cfg }

-- | A lens for '_kkKinesisConfiguration'.
--
kkKinesisConfiguration ∷ Lens' KinesisKit (Kin.KinesisConfiguration Aws.NormalQuery)
kkKinesisConfiguration = lens _kkKinesisConfiguration $ \kk cfg → kk { _kkKinesisConfiguration = cfg }

-- | A lens for '_kkManager'.
--
kkManager ∷ Lens' KinesisKit HC.Manager
kkManager = lens _kkManager $ \kk mgr → kk { _kkManager = mgr }

-- | The minimal effect modality for running Kinesis commands.
--
type MonadKinesis m
  = ( MonadIO m
    , MonadReader KinesisKit m
    )

-- | Run a Kinesis request inside 'MonadKinesis'.
--
runKinesis
  ∷ ( MonadKinesis m
    , Aws.ServiceConfiguration req ~ Kin.KinesisConfiguration
    , Aws.Transaction req resp
    )
  ⇒ req
  → m resp
runKinesis req = do
  KinesisKit{..} ← view id
  eitherT throw return ∘ syncIO ∘ runResourceT $
    Aws.pureAws
      _kkConfiguration
      _kkKinesisConfiguration
      _kkManager
      req

shardIsOpen
  ∷ Kin.Shard
  → Bool
shardIsOpen Kin.Shard{..} =
  isNothing $ shardSequenceNumberRange ^. _2

fetchShardsConduit
  ∷ MonadKinesis m
  ⇒ Kin.StreamName
  → Conduit (Maybe Kin.ShardId) m Kin.Shard
fetchShardsConduit streamName =
  awaitForever $ \mshardId → do
    let req = Kin.DescribeStream
          { Kin.describeStreamExclusiveStartShardId = mshardId
          , Kin.describeStreamLimit = Nothing
          , Kin.describeStreamStreamName = streamName
          }
    resp@(Kin.DescribeStreamResponse Kin.StreamDescription{..}) ←
      lift $ runKinesis req
    yield `mapM_` streamDescriptionShards
    void ∘ traverse (leftover ∘ Just) $
      Kin.describeStreamExclusiveStartShardId =<<
        Aws.nextIteratedRequest req resp
    return ()


-- | A 'Source' of shards for a stream.
--
streamShardSource
  ∷ MonadKinesis m
  ⇒ Kin.StreamName
  → Source m Kin.Shard
streamShardSource streamName =
  CL.sourceList [Nothing] $= fetchShardsConduit streamName

-- | A 'Source' of open shards for a stream.
--
streamOpenShardSource
  ∷ MonadKinesis m
  ⇒ Kin.StreamName
  → Source m Kin.Shard
streamOpenShardSource streamName =
  flip mapOutputMaybe (streamShardSource streamName) $ \shard →
    if shardIsOpen shard
      then Just shard
      else Nothing

-- | Analogous to 'withReader', but supports a result in 'MonadReader'.
--
mapEnvironment
  ∷ MonadReader r' m
  ⇒ Getter r' r
  → ReaderT r m a
  → m a
mapEnvironment l m = view l ≫= runReaderT m

