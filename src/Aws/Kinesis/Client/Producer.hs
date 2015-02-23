-- Copyright (c) 2013-2014 PivotCloud, Inc.
--
-- Aws.Kinesis.Client.Producer
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

{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE UnicodeSyntax #-}

-- |
-- Module: Aws.Kinesis.Client.Producer
-- Copyright: Copyright © 2013-2014 PivotCloud, Inc.
-- License: Apache-2.0
-- Maintainer: Jon Sterling <jsterling@alephcloud.com>
-- Stability: experimental
--
module Aws.Kinesis.Client.Producer
( -- * The Producer
  KinesisProducer
, withKinesisProducer
, managedKinesisProducer

  -- * Commands
, writeProducer
, Message

  -- * Producer Environment
, MonadProducer
, ProducerKit(..)
, pkKinesisKit
, pkStreamName
, pkBatchPolicy
, pkMessageQueueBounds
, pkMaxConcurrency

, ProducerError(..)
, _KinesisError
, _MessageNotEnqueued
, _InvalidConcurrentConsumerCount
, _MessageTooLarge

, pattern MaxMessageSize

, BatchPolicy
, defaultBatchPolicy
, bpBatchSize
, bpEndpoint
, RecordEndpoint(..)
) where

import qualified Aws.Kinesis as Kin
import qualified Aws.Kinesis.Commands.PutRecords as Kin
import Aws.Kinesis.Client.Common

import Control.Applicative
import Control.Concurrent.Async.Lifted
import Control.Concurrent.Lifted hiding (yield)
import Control.Concurrent.STM
import Control.Concurrent.STM.TBMQueue
import Control.Exception.Lifted
import Control.Lens
import Control.Monad
import Control.Monad.Codensity
import Control.Monad.Error.Class
import Control.Monad.Reader
import Control.Monad.Trans.Control
import Control.Monad.Trans.Either
import Data.Conduit
import Data.Conduit.TQueue
import qualified Data.Conduit.List as CL
import Data.Maybe
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Traversable
import Data.Typeable
import Prelude.Unicode
import qualified System.Random as R

-- | There are two endpoints which may be used to send records to Kinesis.
--
data RecordEndpoint
  = PutRecordEndpoint
  -- ^ Use the @PutRecord@ endpoint, which sends records one at a time.

  | PutRecordsEndpoint
  -- ^ Use the @PutRecords@ endpoint, which sends records in batches.

  deriving (Eq, Show)

-- | The maximum size in bytes of a message.
--
pattern MaxMessageSize = 51000

-- | The producer batches records according to a user-specified policy.
--
data BatchPolicy
  = BatchPolicy
  { _bpBatchSize ∷ {-# UNPACK #-} !Int
  , _bpEndpoint ∷ !RecordEndpoint
  } deriving (Eq, Show)

-- | The number of records to send in a single request. This is only used
-- when the endpoint is set to 'PutRecordsEndpoint'.
--
bpBatchSize ∷ Lens' BatchPolicy Int
bpBatchSize = lens _bpBatchSize $ \bp bs → bp { _bpBatchSize = bs }

-- | The endpoint to use when sending records to Kinesis.
--
bpEndpoint ∷ Lens' BatchPolicy RecordEndpoint
bpEndpoint = lens _bpEndpoint $ \bp ep → bp { _bpEndpoint = ep }

-- | The default batching policy sends '200' records per 'PutRecordsEndpoint'
-- request.
--
defaultBatchPolicy ∷ BatchPolicy
defaultBatchPolicy = BatchPolicy
  { _bpBatchSize = 200
  , _bpEndpoint = PutRecordsEndpoint
  }


type Message = T.Text

data MessageQueueItem
  = MessageQueueItem
  { _mqiMessage ∷ !Message
  , _mqiPartitionKey ∷ !Kin.PartitionKey
  } deriving (Eq, Show)

mqiMessage ∷ Lens' MessageQueueItem Message
mqiMessage = lens _mqiMessage $ \i m → i { _mqiMessage = m }

mqiPartitionKey ∷ Lens' MessageQueueItem Kin.PartitionKey
mqiPartitionKey = lens _mqiPartitionKey $ \i s → i { _mqiPartitionKey = s }

-- | The basic input required to construct a Kinesis producer.
--
data ProducerKit
  = ProducerKit
  { _pkKinesisKit ∷ !KinesisKit
  -- ^ The basic information required to send requests to AWS Kinesis.

  , _pkStreamName ∷ !Kin.StreamName
  -- ^ The name of the stream to send records to.

  , _pkBatchPolicy ∷ !BatchPolicy
  -- ^ The record batching policy for the producer.

  , _pkMessageQueueBounds ∷ {-# UNPACK #-} !Int
  -- ^ The maximum number of records that may be enqueued at one time.

  , _pkMaxConcurrency ∷ {-# UNPACK #-} !Int
  -- ^ The number of requests to run concurrently (minimum: 1).
  }

-- | A lens for '_pkKinesisKit'.
--
pkKinesisKit ∷ Lens' ProducerKit KinesisKit
pkKinesisKit = lens _pkKinesisKit $ \pk kk → pk { _pkKinesisKit = kk }

-- | A lens for '_pkStreamName'.
--
pkStreamName ∷ Lens' ProducerKit Kin.StreamName
pkStreamName = lens _pkStreamName $ \pk sn → pk { _pkStreamName = sn }

-- | A lens for '_pkBatchPolicy'.
--
pkBatchPolicy ∷ Lens' ProducerKit BatchPolicy
pkBatchPolicy = lens _pkBatchPolicy $ \pk bp → pk { _pkBatchPolicy = bp }

-- | A lens for '_pkMessageQueueBounds'.
--
pkMessageQueueBounds ∷ Lens' ProducerKit Int
pkMessageQueueBounds = lens _pkMessageQueueBounds $ \pk qb → pk { _pkMessageQueueBounds = qb }

-- | A lens for '_pkMaxConcurrency'.
--
pkMaxConcurrency ∷ Lens' ProducerKit Int
pkMaxConcurrency = lens _pkMaxConcurrency $ \pk n → pk { _pkMaxConcurrency = n }

-- | The (abstract) Kinesis producer client.
--
newtype KinesisProducer
  = KinesisProducer
  { _kpMessageQueue ∷ TBMQueue MessageQueueItem
  }

kpMessageQueue ∷ Getter KinesisProducer (TBMQueue MessageQueueItem)
kpMessageQueue = to _kpMessageQueue

data ProducerError
  = KinesisError !SomeException
  -- ^ Represents an error which occured as a result of a request to Kinesis.

  | MessageNotEnqueued Message
  -- ^ Thrown when a message could not be enqueued since the queue was full.
  -- This error must be handled at the call-site.

  | MessageTooLarge
  -- ^ Thrown when the message was larger than the maximum message size ('MaxMessageSize').

  | InvalidConcurrentConsumerCount
  -- ^ Thrown when 'pkMaxConcurrency' is set with an invalid value.

  deriving (Typeable, Show)

instance Exception ProducerError

-- | A prism for 'KinesisError'.
--
_KinesisError ∷ Prism' ProducerError SomeException
_KinesisError =
  prism KinesisError $ \case
    KinesisError e → Right e
    e → Left e

-- | A prism for 'MessageNotEnqueued'.
--
_MessageNotEnqueued ∷ Prism' ProducerError Message
_MessageNotEnqueued =
  prism MessageNotEnqueued $ \case
    MessageNotEnqueued m → Right m
    e → Left e

-- | A prism for 'MessageTooLarge'.
--
_MessageTooLarge ∷ Prism' ProducerError ()
_MessageTooLarge =
  prism (const MessageTooLarge) $ \case
    MessageTooLarge → Right ()
    e → Left e

-- | A prism for 'InvalidConcurrentConsumerCount'.
--
_InvalidConcurrentConsumerCount ∷ Prism' ProducerError ()
_InvalidConcurrentConsumerCount =
  prism (const InvalidConcurrentConsumerCount) $ \case
    InvalidConcurrentConsumerCount → Right ()
    e → Left e

-- | The basic effect modality required to use the Kinesis producer.
--
type MonadProducer m
  = ( MonadIO m
    , MonadBaseControl IO m
    , MonadError ProducerError m
    )

type MonadProducerInternal m
  = ( MonadProducer m
    , MonadReader ProducerKit m
    )

-- | Lifts something in 'MonadKinesis' to 'MonadProducer'.
--
liftKinesis
  ∷ MonadProducerInternal m
  ⇒ EitherT SomeException (ReaderT KinesisKit m) α
  → m α
liftKinesis =
  mapEnvironment pkKinesisKit
    ∘ mapError KinesisError

-- | Generates a valid 'Kin.PartitionKey'.
--
generatePartitionKey
  ∷ R.RandomGen g
  ⇒ g
  → Kin.PartitionKey
generatePartitionKey gen =
  let name = take 25 $ R.randomRs ('a','z') gen in
  Kin.partitionKey (T.pack name)
    & either (error ∘ T.unpack) id

-- | This will take up to @n@ items from a 'TBMQueue'.
--
takeTBMQueue
  ∷ Int
  → TBMQueue α
  → STM [α]
takeTBMQueue n q
  | n <= 0 = return []
  | otherwise = do
      res ← tryReadTBMQueue q
      case res of
        Just (Just x) → (x:) <$> takeTBMQueue (n - 1) q
        _ → return []

-- | A policy for chunking the contents of the message queue.
--
data ChunkingPolicy
  = ChunkingPolicy
  { _cpMaxChunkSize ∷ Int
  -- ^ The largest chunk size that is permitted.

  , _cpThrottlingDelay ∷ Int
  -- ^ The time after which a chunk should be committed, even if the maximum
  -- chunk size has not yet been reached.
  }

-- | A 'Source' that reads chunks off a bounded STM queue according some
-- 'ChunkingPolicy'.
--
chunkedSourceTBMQueue
  ∷ ( MonadIO m
    , MonadBaseControl IO m
    )
  ⇒ ChunkingPolicy
  → TBMQueue α
  → Source m [α]
chunkedSourceTBMQueue bp@ChunkingPolicy{..} q = do
  terminateNow ← liftIO ∘ atomically $ isClosedTBMQueue q
  unless terminateNow $ do
    items ← liftIO ∘ atomically $ takeTBMQueue _cpMaxChunkSize q
    unless (null items) $ do
      yield items

    when (length items < _cpMaxChunkSize) $
      threadDelay _cpThrottlingDelay

    chunkedSourceTBMQueue bp q

-- | Transform a 'Source' into a chunked 'Source' according to some
-- 'ChunkingPolicy'.
--
chunkSource
  ∷ ( MonadIO m
    , MonadBaseControl IO m
    )
  ⇒ ChunkingPolicy
  → Source m α
  → Source m [α]
chunkSource cp src = do
  queue ← liftIO $ newTBMQueueIO $ _cpMaxChunkSize cp
  worker ← lift ∘ async $ src $$+ sinkTBMQueue queue True
  addCleanup (\_ → cancel worker) $ do
    lift $ link worker
    chunkedSourceTBMQueue cp queue
  return ()

-- | A conduit for concurently sending multiple records to Kinesis using the
-- @PutRecord@ endpoint.
--
concurrentPutRecordSink
  ∷ MonadProducerInternal m
  ⇒ Sink [MessageQueueItem] m ()
concurrentPutRecordSink = do
  maxWorkerCount ← view pkMaxConcurrency
  awaitForever $ \messages → do
    lift ∘ flip (mapConcurrentlyN maxWorkerCount 100) messages $ \m → do
      CL.sourceList [m] $$ putRecordSink


-- | A conduit for sending a record to Kinesis using the @PutRecord@ endpoint;
-- this is a conduit in order to restore failed messages as leftovers.
--
putRecordSink
  ∷ MonadProducerInternal m
  ⇒ Sink MessageQueueItem m ()
putRecordSink = do
  streamName ← view pkStreamName

  awaitForever $ \item → do
    let handler e = do
          liftIO $ do
            putStrLn $ "Error: " ++ show e
            putStrLn "Will wait 5s"
            threadDelay 5000000
          leftover item

    handleError handler $ do
      let partitionKey = item ^. mqiPartitionKey
      void ∘ lift ∘ liftKinesis $ runKinesis Kin.PutRecord
        { Kin.putRecordData = item ^. mqiMessage ∘ to T.encodeUtf8
        , Kin.putRecordExplicitHashKey = Nothing
        , Kin.putRecordPartitionKey = partitionKey
        , Kin.putRecordSequenceNumberForOrdering = Nothing
        , Kin.putRecordStreamName = streamName
        }

splitEvery
  ∷ Int
  → [α]
  → [[α]]
splitEvery _ [] = []
splitEvery n list = first : splitEvery n rest
  where
    (first,rest) = splitAt n list

-- | A conduit for sending records to Kinesis using the @PutRecords@ endpoint.
-- This is a conduit in order to restore failed messages as leftovers.
--
putRecordsSink
  ∷ MonadProducerInternal m
  ⇒ Sink [MessageQueueItem] m ()
putRecordsSink = do
  streamName ← view pkStreamName
  batchSize ← view $ pkBatchPolicy ∘ bpBatchSize
  maxWorkerCount ← view pkMaxConcurrency
  awaitForever $ \messages → do
    let batches = splitEvery batchSize messages
    leftovers ← lift ∘ flip (mapConcurrentlyN maxWorkerCount 100) batches $ \ms → do
      let handler e = do
            liftIO $ print e
            return ms

      handleError handler $ do
        items ← for ms $ \m → do
          let partitionKey = m ^. mqiPartitionKey
          return Kin.PutRecordsRequestEntry
            { Kin.putRecordsRequestEntryData = m ^. mqiMessage ∘ to T.encodeUtf8
            , Kin.putRecordsRequestEntryExplicitHashKey = Nothing
            , Kin.putRecordsRequestEntryPartitionKey = partitionKey
            }

        Kin.PutRecordsResponse{..} ←  liftKinesis $ runKinesis Kin.PutRecords
          { Kin.putRecordsRecords = items
          , Kin.putRecordsStreamName = streamName
          }
        let processResult m m'
              | isJust (Kin.putRecordsResponseRecordErrorCode m') = Just m
              | otherwise = Nothing
        return ∘ catMaybes $ zipWith processResult ms putRecordsResponseRecords

    forM_ leftovers $ \mss →
      unless (null mss) $ leftover mss

sendMessagesSink
  ∷ MonadProducerInternal m
  ⇒ Sink [MessageQueueItem] m ()
sendMessagesSink = do
  batchPolicy ← view pkBatchPolicy
  case batchPolicy ^. bpEndpoint of
    PutRecordsEndpoint → putRecordsSink
    PutRecordEndpoint → concurrentPutRecordSink

-- | Enqueues a message to Kinesis on the next shard. If a message cannot be
-- enqueued (because the client has exceeded its queue size), the
-- 'MessageNotEnqueued' exception will be thrown.
--
writeProducer
  ∷ MonadProducer m
  ⇒ KinesisProducer
  → Message
  → m ()
writeProducer producer !msg = do
  when (T.length msg > MaxMessageSize) $
    throwError MessageTooLarge

  gen ← liftIO R.newStdGen
  result ← liftIO ∘ atomically $ do
    tryWriteTBMQueue (producer ^. kpMessageQueue) MessageQueueItem
      { _mqiMessage = msg
      , _mqiPartitionKey = generatePartitionKey gen
      }
  case result of
    Just True → return ()
    _ → throwError $ MessageNotEnqueued msg

-- | This is a 'Source' that returns all the items presently in a queue: it
-- terminates when the queue is empty.
--
exhaustTBMQueue
  ∷ TBMQueue α
  → Source STM α
exhaustTBMQueue q = do
  mx ← lift $ tryReadTBMQueue q
  case mx of
    Just (Just x) → do
      yield x
      exhaustTBMQueue q
    _ → return ()

-- | This constructs a 'KinesisProducer' and closes it when you have done with
-- it. This is equivalent to 'withKinesisProducer', but replaces the
-- continuation with a return in 'Codensity'.
--
managedKinesisProducer
  ∷ ∀ m
  . ( MonadIO m
    , MonadBaseControl IO m
    , MonadError ProducerError m
    )
  ⇒ ProducerKit
  → Codensity m KinesisProducer
managedKinesisProducer kit = do
  when (kit ^. pkMaxConcurrency < 1) ∘ lift $
    throwError InvalidConcurrentConsumerCount

  messageQueue ← liftIO ∘ newTBMQueueIO $ kit ^. pkMessageQueueBounds

  let chunkingPolicy = ChunkingPolicy ((kit ^. pkBatchPolicy ∘ bpBatchSize) * (kit ^. pkMaxConcurrency)) 5000000
      -- TODO: this 'forever' is only here to restart if we get killed.
      -- Replace with proper error handling.
      consumerLoop ∷ m () = flip runReaderT kit ∘ forever $
        chunkSource chunkingPolicy (sourceTBMQueue messageQueue)
          $$ sendMessagesSink

  let cleanupConsumer consumerHandle = do
        liftIO ∘ atomically $ closeTBMQueue messageQueue
        flip runReaderT kit $ do
          leftovers ← liftIO ∘ atomically $
            exhaustTBMQueue messageQueue
              $$ CL.consume
          chunkSource chunkingPolicy (CL.sourceList leftovers)
            $$ sendMessagesSink
        cancel consumerHandle

  consumerHandle ← managedBracket (async consumerLoop) cleanupConsumer

  Codensity $ \inner → do
    link consumerHandle
    res ← inner $ KinesisProducer messageQueue
    () ← wait consumerHandle
    return res


-- | This constructs a 'KinesisProducer' and closes it when you have done with
-- it.
--
withKinesisProducer
  ∷ ( MonadIO m
    , MonadBaseControl IO m
    , MonadError ProducerError m
    )
  ⇒ ProducerKit
  → (KinesisProducer → m α)
  → m α
withKinesisProducer =
  runCodensity ∘ managedKinesisProducer

managedBracket
  ∷ MonadBaseControl IO m
  ⇒ m α
  → (α → m β)
  → Codensity m α
managedBracket action cleanup =
  Codensity $ bracket action cleanup

-- | map at most n actions concurrently
--
mapConcurrentlyN
  ∷ ( MonadIO m
    , MonadBaseControl IO m
    , Traversable t
    )
  ⇒ Int -- ^ number of concurrent actions
  → Int -- ^ startup delay between actions in milliseconds
  → (a → m b)
  → t a
  → m (t b)
mapConcurrentlyN n delay f t = do
  sem ← liftIO $ newQSem n
  mapConcurrently (run sem) t_
    where
      (_, t_) = mapAccumL (\i v → (succ i, (i,v))) 0 t
      run sem (i,a) =
        liftBaseOp_ (bracket_ (waitQSem sem) (signalQSem sem)) $ do
          liftIO ∘ threadDelay $ 1000 * delay * i
          f a

