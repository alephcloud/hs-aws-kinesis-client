-- Copyright (c) 2013-2015 PivotCloud, Inc.
--
-- Aws.Kinesis.Client.Consumer.Internal
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

{-# LANGUAGE CPP #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE UnicodeSyntax #-}

-- |
-- Module: Aws.Kinesis.Client.Consumer.Internal
-- Copyright: Copyright © 2013-2015 PivotCloud, Inc.
-- License: Apache-2.0
-- Maintainer: Jon Sterling <jsterling@alephcloud.com>
-- Stability: experimental
--
module Aws.Kinesis.Client.Consumer.Internal
( -- * Types
  MessageQueueItem
, MessageQueue
, StreamState

  -- * Operations
, updateStreamState
, replenishMessages

  -- * Re-exports
, module Aws.Kinesis.Client.Consumer.Internal.Kit
, module Aws.Kinesis.Client.Consumer.Internal.ShardState
, module Aws.Kinesis.Client.Consumer.Internal.SavedStreamState
) where

import Aws.Kinesis
import Aws.Kinesis.Client.Common
import Aws.Kinesis.Client.Consumer.Internal.Kit
import Aws.Kinesis.Client.Consumer.Internal.ShardState
import Aws.Kinesis.Client.Consumer.Internal.SavedStreamState

import Control.Lens
import Control.Lens.Action
import Control.Concurrent.STM
import Control.Concurrent.STM.Queue
import Control.Monad
import Control.Monad.Trans
import qualified Data.Carousel as CR
import Data.Conduit
import qualified Data.Conduit.List as CondL
import Prelude.Unicode

#ifdef DEBUG
import Data.Monoid.Unicode
import System.IO
#else
#endif

type MessageQueueItem = (ShardState, Record)
type MessageQueue = TBQueue MessageQueueItem
type StreamState = CR.Carousel ShardState

-- | This requests new information from Kinesis and reconciles that with an
-- existing carousel of shard states.
--
updateStreamState
  ∷ ConsumerKit
  → StreamState
  → IO StreamState
updateStreamState ConsumerKit{..} state = do
  let
    existingShardIds = state ^. CR.clList <&> view ssShardId
    shardSource =
      flip mapOutputMaybe (streamOpenShardSource _ckKinesisKit _ckStreamName) $ \sh@Shard{..} →
        if shardShardId ∈ existingShardIds
          then Nothing
          else Just sh

  newShards ← shardSource $$ CondL.consume
  shardStates ← forM newShards $ \Shard{..} → do
    let
      startingSequenceNumber =
        _ckSavedStreamState ^? _Just ∘ _SavedStreamState ∘ ix shardShardId
      iteratorType =
        maybe
          _ckIteratorType
          (const AfterSequenceNumber)
          startingSequenceNumber

#ifdef DEBUG
    debugPrint stdout $ "Getting " ⊕ show iteratorType ⊕ " iterator for shard " ⊕ show shardShardId
#else
    return ()
#endif

    GetShardIteratorResponse it ← runKinesis _ckKinesisKit GetShardIterator
      { getShardIteratorShardId = shardShardId
      , getShardIteratorShardIteratorType = iteratorType
      , getShardIteratorStartingSequenceNumber = startingSequenceNumber
      , getShardIteratorStreamName = _ckStreamName
      }

    liftIO ∘ atomically $ do
      iteratorVar ← newTVar $ Just it
      sequenceNumberVar ← newTVar startingSequenceNumber
      return $ makeShardState shardShardId iteratorVar sequenceNumberVar

  return ∘ CR.nub $ CR.append shardStates state

-- | Waits for a message queue to be emptied and fills it up again.
--
replenishMessages
  ∷ ConsumerKit
  → MessageQueue
  → TVar StreamState
  → IO Int
replenishMessages ConsumerKit{..} messageQueue shardsVar = do
  liftIO ∘ atomically ∘ awaitQueueEmpty $ messageQueue
  (shard, iterator) ← liftIO ∘ atomically $ do
    mshard ← shardsVar ^!? act readTVar ∘ CR.cursor
    shard ← maybe retry return mshard
    miterator ← shard ^! ssIterator ∘ act readTVar
    iterator ← maybe retry return miterator
    return (shard, iterator)

  GetRecordsResponse{..} ← runKinesis _ckKinesisKit GetRecords
    { getRecordsLimit = Just $ fromIntegral _ckBatchSize
    , getRecordsShardIterator = iterator
    }

#ifdef DEBUG
  debugPrint stdout $
    "Replenished shard "
      ⊕ show (shard ^. ssShardId)
      ⊕ " with "
      ⊕ show (length getRecordsResRecords)
      ⊕ " records"
#else
  return ()
#endif

  liftIO ∘ atomically $ do
    writeTVar (shard ^. ssIterator) getRecordsResNextShardIterator
    forM_ getRecordsResRecords $ writeTBQueue messageQueue ∘ (shard ,)
    modifyTVar shardsVar CR.moveRight

  return $ length getRecordsResRecords

#ifdef DEBUG
debugPrint
  ∷ MonadIO m
  ⇒ Handle
  → String
  → m ()
debugPrint h =
  liftIO
    ∘ hPutStrLn h
    ∘ ("[Kinesis Consumer] " ⊕)
#else
#endif
