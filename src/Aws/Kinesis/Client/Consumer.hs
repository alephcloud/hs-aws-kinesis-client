-- Copyright (c) 2013-2014 PivotCloud, Inc.
--
-- Aws.Kinesis.Client.Consumer
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
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UnicodeSyntax #-}

-- |
-- Module: Aws.Kinesis.Client.Consumer
-- Copyright: Copyright © 2013-2014 PivotCloud, Inc.
-- License: Apache-2.0
-- Maintainer: Jon Sterling <jsterling@alephcloud.com>
-- Stability: experimental
--
module Aws.Kinesis.Client.Consumer
( -- * The Consumer
  KinesisConsumer
, managedKinesisConsumer
, withKinesisConsumer

  -- * Commands
, consumerSource
, readConsumer
, tryReadConsumer
, consumerStreamState

  -- * Consumer Environment
, ConsumerKit(..)
, ckKinesisKit
, ckStreamName
, ckBatchSize
, ckIteratorType
, ckSavedStreamState
, ConsumerError(..)
, SavedStreamState
) where

import qualified Aws.Kinesis as Kin
import Aws.Kinesis.Client.Common

import Control.Applicative
import Control.Applicative.Unicode
import Control.Concurrent.Async.Lifted
import Control.Concurrent.Lifted hiding (yield)
import Control.Concurrent.STM
import Control.Concurrent.STM.Queue
import Control.Exception.Lifted
import Control.Lens
import Control.Lens.Action
import Control.Monad.Codensity
import Control.Monad.Reader
import Control.Monad.Trans.Control
import Control.Monad.Unicode
import qualified Data.Aeson as Æ
import qualified Data.Carousel as CR
import qualified Data.Map as M
import qualified Data.HashMap.Strict as HM
import Data.Traversable (for)
import Data.Conduit
import qualified Data.Conduit.List as CL
import Prelude.Unicode

-- | The internal representation for shards used by the consumer.
--
data ShardState
  = ShardState
  { _ssIterator ∷ !(TVar (Maybe Kin.ShardIterator))
  , _ssShardId ∷ !Kin.ShardId
  , _ssLastSequenceNumber ∷ !(TVar (Maybe Kin.SequenceNumber))
  }

-- | A getter for '_ssIterator'.
--
ssIterator ∷ Getter ShardState (TVar (Maybe Kin.ShardIterator))
ssIterator = to _ssIterator

-- | A lens for '_ssShardId'.
--
ssShardId ∷ Lens' ShardState Kin.ShardId
ssShardId = lens _ssShardId $ \ss sid → ss { _ssShardId = sid }

-- | A getter for '_ssLastSequenceNumber'.
--
ssLastSequenceNumber ∷ Getter ShardState (TVar (Maybe Kin.SequenceNumber))
ssLastSequenceNumber = to _ssLastSequenceNumber

-- | 'ShardState' is quotiented by shard ID.
--
instance Eq ShardState where
  ss == ss' = ss ^. ssShardId ≡ ss' ^. ssShardId

data ConsumerError
  = NoShards
  -- ^ A stream must always have at least one open shard.
  deriving Show

-- | The 'ConsumerKit' contains what is needed to initialize a 'KinesisConsumer'.
data ConsumerKit
  = ConsumerKit
  { _ckKinesisKit ∷ !KinesisKit
  -- ^ The credentials and configuration for making requests to AWS Kinesis.

  , _ckStreamName ∷ !Kin.StreamName
  -- ^ The name of the stream to consume from.

  , _ckBatchSize ∷ {-# UNPACK #-} !Int
  -- ^ The number of records to fetch at once from the stream.

  , _ckIteratorType ∷ !Kin.ShardIteratorType
  -- ^ The type of iterator to consume.

  , _ckSavedStreamState ∷ !(Maybe SavedStreamState)
  -- ^ Optionally, an initial stream state. The iterator type in
  -- '_ckIteratorType' will be used for any shards not present in the saved
  -- stream state; otherwise, 'Kin.AfterSequenceNumber' will be used.
  }

-- | A lens for '_ckKinesisKit'.
--
ckKinesisKit ∷ Lens' ConsumerKit KinesisKit
ckKinesisKit = lens _ckKinesisKit $ \ck kk → ck { _ckKinesisKit = kk }

-- | A lens for '_ckStreamName'.
--
ckStreamName ∷ Lens' ConsumerKit Kin.StreamName
ckStreamName = lens _ckStreamName $ \ck sn → ck { _ckStreamName = sn }

-- | A lens for '_ckBatchSize'.
--
ckBatchSize ∷ Lens' ConsumerKit Int
ckBatchSize = lens _ckBatchSize $ \ck bs → ck { _ckBatchSize = bs }

-- | A lens for '_ckIteratorType'.
--
ckIteratorType ∷ Lens' ConsumerKit Kin.ShardIteratorType
ckIteratorType = lens _ckIteratorType $ \ck it → ck { _ckIteratorType = it }

-- | A lens for '_ckSavedStreamState'.
--
ckSavedStreamState ∷ Lens' ConsumerKit (Maybe SavedStreamState)
ckSavedStreamState = lens _ckSavedStreamState $ \ck ss → ck { _ckSavedStreamState = ss }


type MessageQueueItem = (ShardState, Kin.Record)
type StreamState = CR.Carousel ShardState

newtype SavedStreamState
  = SavedStreamState
  { _savedStreamState ∷ M.Map Kin.ShardId Kin.SequenceNumber
  }

-- | An iso for 'SavedStreamState'.
--
_SavedStreamState ∷ Iso' SavedStreamState (M.Map Kin.ShardId Kin.SequenceNumber)
_SavedStreamState = iso _savedStreamState SavedStreamState

instance Æ.ToJSON SavedStreamState where
  toJSON (SavedStreamState m) =
    Æ.Object ∘ HM.fromList ∘ flip fmap (M.toList m) $ \(sid, sn) →
      let Æ.String sid' = Æ.toJSON sid
      in sid' Æ..= sn

instance Æ.FromJSON SavedStreamState where
  parseJSON =
    Æ.withObject "SavedStreamState" $ \xs → do
      fmap (SavedStreamState ∘ M.fromList) ∘ for (HM.toList xs) $ \(sid, sn) → do
        pure (,)
          ⊛ Æ.parseJSON (Æ.String sid)
          ⊛ Æ.parseJSON sn


-- | The 'KinesisConsumer' maintains state about which shards to pull from.
--
data KinesisConsumer
  = KinesisConsumer
  { _kcMessageQueue ∷ !(TBQueue MessageQueueItem)
  , _kcStreamState ∷ !(TVar StreamState)
  }

-- | A getter for '_kcMessageQueue'.
--
kcMessageQueue ∷ Getter KinesisConsumer (TBQueue MessageQueueItem)
kcMessageQueue = to _kcMessageQueue

-- | A getter for '_kcStreamState'.
--
kcStreamState ∷ Getter KinesisConsumer (TVar StreamState)
kcStreamState = to _kcStreamState

type MonadConsumerInternal m
  = ( MonadIO m
    , MonadBaseControl IO m
    , MonadReader ConsumerKit m
    )

-- | This constructs a 'KinesisConsumer' and closes it when you have done with
-- it; this is equivalent to 'withKinesisConsumer', except that the
-- continuation is replaced with returning the consumer in 'Codensity'.
--
managedKinesisConsumer
  ∷ ( MonadIO m
    , MonadBaseControl IO m
    )
  ⇒ ConsumerKit
  → Codensity m KinesisConsumer
managedKinesisConsumer kit =
  Codensity $ withKinesisConsumer kit

-- | This constructs a 'KinesisConsumer' and closes it when you have done with
-- it.
--
withKinesisConsumer
  ∷ ( MonadIO m
    , MonadBaseControl IO m
    )
  ⇒ ConsumerKit
  → (KinesisConsumer → m α)
  → m α
withKinesisConsumer kit inner =
  flip runReaderT kit $ do
    batchSize ← view ckBatchSize
    messageQueue ← liftIO ∘ newTBQueueIO $ batchSize * 10

    state ← updateStreamState CR.empty ≫= liftIO ∘ newTVarIO

    let
      reshardingLoop =
        forever ∘ handle (\(SomeException _) → liftIO $ threadDelay 3000000) $ do
          liftIO (readTVarIO state)
            ≫= updateStreamState
            ≫= liftIO ∘ atomically ∘ writeTVar state
          liftIO $ threadDelay 10000000

      producerLoop =
        forever ∘ handle (\(SomeException _) → liftIO $ threadDelay 2000000) $ do
          recordsCount ← replenishMessages messageQueue state
          liftIO ∘ threadDelay $
            case recordsCount of
              0 → 5000000
              _ → 70000


    withAsync reshardingLoop $ \reshardingHandle → do
      link reshardingHandle
      withAsync producerLoop $ \producerHandle → do
        link producerHandle
        res ← lift ∘ inner $ KinesisConsumer messageQueue state
        return res

-- | This requests new information from Kinesis and reconciles that with an
-- existing carousel of shard states.
--
updateStreamState
  ∷ MonadConsumerInternal m
  ⇒ StreamState
  → m StreamState
updateStreamState state = do
  ConsumerKit{..} ← ask

  let
    existingShardIds = state ^. CR.clList <&> view ssShardId
    shardSource =
      flip mapOutputMaybe (streamOpenShardSource _ckKinesisKit _ckStreamName) $ \sh@Kin.Shard{..} →
        if shardShardId `elem` existingShardIds
          then Nothing
          else Just sh

  newShards ← shardSource $$ CL.consume
  shardStates ← forM newShards $ \Kin.Shard{..} → do
    let
      startingSequenceNumber =
        _ckSavedStreamState ^? _Just ∘ _SavedStreamState ∘ ix shardShardId
      iteratorType =
        maybe
          _ckIteratorType
          (const Kin.AfterSequenceNumber)
          startingSequenceNumber

    Kin.GetShardIteratorResponse it ← runKinesis _ckKinesisKit Kin.GetShardIterator
      { Kin.getShardIteratorShardId = shardShardId
      , Kin.getShardIteratorShardIteratorType = iteratorType
      , Kin.getShardIteratorStartingSequenceNumber = startingSequenceNumber
      , Kin.getShardIteratorStreamName = _ckStreamName
      }
    liftIO $ do
      iteratorVar ← newTVarIO $ Just it
      sequenceNumberVar ← newTVarIO Nothing
      return ShardState
        { _ssIterator = iteratorVar
        , _ssShardId = shardShardId
        , _ssLastSequenceNumber = sequenceNumberVar
        }
  return ∘ CR.nub $ CR.append shardStates state

-- | Waits for a message queue to be emptied and fills it up again.
--
replenishMessages
  ∷ MonadConsumerInternal m
  ⇒ TBQueue MessageQueueItem
  → TVar StreamState
  → m Int
replenishMessages messageQueue shardsVar = do
  bufferSize ← view ckBatchSize
  liftIO ∘ atomically ∘ awaitQueueEmpty $ messageQueue
  (shard, iterator) ← liftIO ∘ atomically $ do
    mshard ← shardsVar ^!? act readTVar ∘ CR.cursor
    shard ← maybe retry return mshard
    miterator ← shard ^! ssIterator ∘ act readTVar
    iterator ← maybe retry return miterator
    return (shard, iterator)

  kinesisKit ← view ckKinesisKit
  Kin.GetRecordsResponse{..} ← runKinesis kinesisKit Kin.GetRecords
    { getRecordsLimit = Just bufferSize
    , getRecordsShardIterator = iterator
    }

  liftIO ∘ atomically $ do
    writeTVar (shard ^. ssIterator) getRecordsResNextShardIterator
    forM_ getRecordsResRecords $ writeTBQueue messageQueue ∘ (shard ,)
    modifyTVar shardsVar CR.moveRight

  return $ length getRecordsResRecords

-- | Await and read a single record from the consumer.
--
readConsumer
  ∷ ( MonadIO m
    , MonadBaseControl IO m
    )
  ⇒ KinesisConsumer
  → m Kin.Record
readConsumer consumer =
  liftIO ∘ atomically $ do
    (ss, rec) ← consumer ^! kcMessageQueue ∘ act readTBQueue
    writeTVar (ss ^. ssLastSequenceNumber) ∘ Just $ Kin.recordSequenceNumber rec
    return rec

-- | Try to read a single record from the consumer; if there is non queued up,
-- then 'Nothing' will be returned.
--
tryReadConsumer
  ∷ ( MonadIO m
    , MonadBaseControl IO m
    )
  ⇒ KinesisConsumer
  → m (Maybe Kin.Record)
tryReadConsumer consumer =
  liftIO ∘ atomically $ do
    mitem ← consumer ^! kcMessageQueue ∘ act tryReadTBQueue
    for mitem $ \(ss, rec) → do
      writeTVar (ss ^. ssLastSequenceNumber) ∘ Just $ Kin.recordSequenceNumber rec
      return rec

-- | A conduit for getting records.
--
consumerSource
  ∷ ( MonadIO m
    , MonadBaseControl IO m
    )
  ⇒ KinesisConsumer
  → Source m Kin.Record
consumerSource consumer =
  forever $
    lift (readConsumer consumer)
      ≫= yield

-- | Get the last read sequence number at each shard.
--
consumerStreamState
  ∷ ( MonadIO m
    , MonadBaseControl IO m
    )
  ⇒ KinesisConsumer
  → m SavedStreamState
consumerStreamState consumer =
  liftIO ∘ atomically $ do
    shards ← consumer ^! kcStreamState ∘ act readTVar ∘ CR.clList
    pairs ← for shards $ \ss →
      (ss ^. ssShardId,) <$>
        ss ^! ssLastSequenceNumber ∘ act readTVar
    return ∘ SavedStreamState ∘ M.fromList $ pairs ≫= \(sid, msn) →
      msn ^.. _Just ∘ to (sid,)
