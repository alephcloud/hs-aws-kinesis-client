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
, ProducerKit(..)
, makeProducerKit
  -- ** Queue Implementations
, QueueImplementation(..)
, defaultQueueImplementation

  -- ** Lenses
, pkKinesisKit
, pkStreamName
, pkBatchPolicy
, pkRetryPolicy
, pkMessageQueueBounds
, pkMaxConcurrency
, pkCleanupTimeout
, pkQueueImplementation

-- * Exceptions
, WriteProducerException(..)
, ProducerCleanupTimedOut(..)
, ProducerWorkerDied(..)

, pattern MaxMessageSize

-- * Policies
, BatchPolicy
, defaultBatchPolicy
, bpBatchSize

, RetryPolicy
, defaultRetryPolicy
, rpRetryCount
) where

import qualified Aws.Kinesis as Kin
import Aws.Kinesis.Client.Common
import Aws.Kinesis.Client.Queue

import Control.Applicative
import Control.Concurrent.Async.Lifted
import Control.Concurrent.Lifted hiding (yield)
import Control.Concurrent.STM.TBMChan
import Control.Exception.Enclosed
import Control.Exception.Lifted
import Control.Lens
import Control.Monad
import Control.Monad.Codensity
import Control.Monad.Reader
import Control.Monad.Trans.Control
import Control.Monad.Trans.Except
import Data.Conduit
import Data.Maybe
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Traversable
import Data.Typeable
import Numeric.Natural
import Prelude.Unicode
import qualified System.Random as R
import System.IO

-- | The maximum size in bytes of a message.
--
pattern MaxMessageSize = 51000

-- | The producer batches records according to a user-specified policy.
--
data BatchPolicy
  = BatchPolicy
  { _bpBatchSize ∷ {-# UNPACK #-} !Natural
  } deriving (Eq, Show)

-- | The number of records to send in a single request. This is only used
-- when the endpoint is set to 'PutRecordsEndpoint'.
--
bpBatchSize ∷ Lens' BatchPolicy Natural
bpBatchSize = lens _bpBatchSize $ \bp bs → bp { _bpBatchSize = bs }

-- | The default batching policy sends @200@ records per 'PutRecordsEndpoint'
-- request.
--
defaultBatchPolicy ∷ BatchPolicy
defaultBatchPolicy = BatchPolicy
  { _bpBatchSize = 200
  }

-- | The producer will attempt to re-send records which failed according to a
-- user-specified policy. This policy applies to failures which occur in the
-- process of sending a message to Kinesis, not those which occur in the course
-- of enqueuing a message.
data RetryPolicy
  = RetryPolicy
  { _rpRetryCount ∷ {-# UNPACK #-} !Natural
  } deriving (Eq, Show)

-- | The number of times to retry sending a message after it has first failed.
--
rpRetryCount ∷ Lens' RetryPolicy Natural
rpRetryCount = lens _rpRetryCount $ \rp n → rp { _rpRetryCount = n }

-- | The default retry policy will attempt @5@ retries for a message.
--
defaultRetryPolicy ∷ RetryPolicy
defaultRetryPolicy = RetryPolicy
  { _rpRetryCount = 5
  }

type Message = T.Text

data MessageQueueItem
  = MessageQueueItem
  { _mqiMessage ∷ !Message
  -- ^ The contents of the message

  , _mqiPartitionKey ∷ !Kin.PartitionKey
  -- ^ The partition key the message is destined for

  , _mqiRemainingAttempts ∷ {-# UNPACK #-} !Natural
  -- ^ The number of times remaining to try and publish this message
  } deriving (Eq, Show)

mqiMessage ∷ Lens' MessageQueueItem Message
mqiMessage = lens _mqiMessage $ \i m → i { _mqiMessage = m }

mqiPartitionKey ∷ Lens' MessageQueueItem Kin.PartitionKey
mqiPartitionKey = lens _mqiPartitionKey $ \i s → i { _mqiPartitionKey = s }

mqiRemainingAttempts ∷ Lens' MessageQueueItem Natural
mqiRemainingAttempts = lens _mqiRemainingAttempts $ \i n → i { _mqiRemainingAttempts = n }

messageQueueItemIsEligible
  ∷ MessageQueueItem
  → Bool
messageQueueItemIsEligible =
  (≥ 1) ∘ _mqiRemainingAttempts

data QueueImplementation
  = ∀ q. BoundedCloseableQueue q MessageQueueItem
  ⇒ QueueImplementation (Proxy q)

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

  , _pkRetryPolicy ∷ !RetryPolicy
  -- ^ The retry policy for the producer.

  , _pkMessageQueueBounds ∷ {-# UNPACK #-} !Natural
  -- ^ The maximum number of records that may be enqueued at one time.

  , _pkMaxConcurrency ∷ {-# UNPACK #-} !Natural
  -- ^ The number of requests to run concurrently (minimum: 1).

  , _pkCleanupTimeout ∷ !(Maybe Natural)
  -- ^ The timeout in milliseconds, after which the producer's cleanup routine
  -- will terminate, finished or not, throwing 'ProducerCleanupTimedOut'.

  , _pkQueueImplementation ∷ QueueImplementation
  -- ^ The Kinesis Producer is parameterized over a concrete queue implementation.
  }

-- | Create a 'ProducerKit' with default settings.
--
makeProducerKit
  ∷ KinesisKit
  → Kin.StreamName
  → ProducerKit
makeProducerKit kinesisKit streamName = ProducerKit
  { _pkKinesisKit = kinesisKit
  , _pkStreamName = streamName
  , _pkBatchPolicy = defaultBatchPolicy
  , _pkRetryPolicy = defaultRetryPolicy
  , _pkMessageQueueBounds = 10000
  , _pkMaxConcurrency = 3
  , _pkCleanupTimeout = Nothing
  , _pkQueueImplementation = defaultQueueImplementation
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

-- | A lens for '_pkRetryPolicy'.
--
pkRetryPolicy ∷ Lens' ProducerKit RetryPolicy
pkRetryPolicy = lens _pkRetryPolicy $ \pk rp → pk { _pkRetryPolicy = rp }

-- | A lens for '_pkMessageQueueBounds'.
--
pkMessageQueueBounds ∷ Lens' ProducerKit Natural
pkMessageQueueBounds = lens _pkMessageQueueBounds $ \pk qb → pk { _pkMessageQueueBounds = qb }

-- | A lens for '_pkMaxConcurrency'.
--
pkMaxConcurrency ∷ Lens' ProducerKit Natural
pkMaxConcurrency = lens _pkMaxConcurrency $ \pk n → pk { _pkMaxConcurrency = n }

-- | A lens for '_pkCleanupTimeout'.
--
pkCleanupTimeout ∷ Lens' ProducerKit (Maybe Natural)
pkCleanupTimeout = lens _pkCleanupTimeout $ \pk n → pk { _pkCleanupTimeout = n }

-- | A lens for '_pkQueueImplementation'.
--
pkQueueImplementation ∷ Setter' ProducerKit QueueImplementation
pkQueueImplementation = lens _pkQueueImplementation $ \pk q → pk { _pkQueueImplementation = q }

-- | The (abstract) Kinesis producer client.
--
data KinesisProducer
  = ∀ q. BoundedCloseableQueue q MessageQueueItem
  ⇒ KinesisProducer
  { _kpMessageQueue ∷ !q
  , _kpRetryPolicy ∷ !RetryPolicy
  }

data WriteProducerException
  = ProducerQueueClosed
    -- ^ Thrown when a message could not be enqueued since the queue was closed.
  | ProducerQueueFull
    -- ^ Thrown when a message could not be enqueued since the queue was full.
  | MessageTooLarge
    -- ^ Thrown when the message was larger than the maximum message size
    -- ('MaxMessageSize')
  deriving (Typeable, Show, Eq)

instance Exception WriteProducerException

-- | Thrown when the producer's cleanup routine takes longer than the
-- configured timeout.
data ProducerCleanupTimedOut
  = ProducerCleanupTimedOut
  deriving (Typeable, Show, Eq)

instance Exception ProducerCleanupTimedOut

-- | Thrown when the producer's worker dies unexpectedly (this is fatal, and
-- should never happen).
data ProducerWorkerDied
  = ProducerWorkerDied (Maybe SomeException)
  deriving (Typeable, Show)

instance Exception ProducerWorkerDied

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

-- | A policy for chunking the contents of the message queue.
--
data ChunkingPolicy
  = ChunkingPolicy
  { _cpMaxChunkSize ∷ !Natural
  -- ^ The largest chunk size that is permitted.

  , _cpMinChunkingInterval ∷ !Natural
  -- ^ The time in microseconds after which a chunk should be committed, even
  -- if the maximum chunk size has not yet been reached.
  }

-- | A 'Source' that reads chunks off a bounded STM queue according some
-- 'ChunkingPolicy'.
--
chunkedSourceFromQueue
  ∷ BoundedCloseableQueue q α
  ⇒ ChunkingPolicy
  → q
  → Source IO [α]
chunkedSourceFromQueue cp@ChunkingPolicy{..} q = do
  shouldTerminate ← liftIO $ isClosedAndEmptyQueue q
  unless shouldTerminate $ do
    items ← lift $ takeQueueTimeout q _cpMaxChunkSize _cpMinChunkingInterval
    unless (null items) $ do
      yield items

    chunkedSourceFromQueue cp q

splitEvery
  ∷ Natural
  → [α]
  → [[α]]
splitEvery _ [] = []
splitEvery n list = first : splitEvery n rest
  where
    (first,rest) = splitAt (fromIntegral n) list

-- | A conduit for sending records to Kinesis using the @PutRecords@ endpoint.
-- This is a conduit in order to restore failed messages as leftovers.
--
putRecordsSink
  ∷ ProducerKit
  → Sink [MessageQueueItem] IO ()
putRecordsSink ProducerKit{..} = do
  let batchSize = _pkBatchPolicy ^. bpBatchSize

  awaitForever $ \messages → do
    let batches = splitEvery batchSize messages
    leftovers ← lift ∘ flip (mapConcurrentlyN _pkMaxConcurrency 100) batches $ \items → do
      case filter messageQueueItemIsEligible items of
        [] → return []
        eligibleItems → do
          handleAny (\(SomeException e) → eligibleItems <$ liftIO (hPutStrLn stderr $ show e)) $ do
            requestEntries ← for eligibleItems $ \m → do
              let partitionKey = m ^. mqiPartitionKey
              return Kin.PutRecordsRequestEntry
                { Kin.putRecordsRequestEntryData = m ^. mqiMessage ∘ to T.encodeUtf8
                , Kin.putRecordsRequestEntryExplicitHashKey = Nothing
                , Kin.putRecordsRequestEntryPartitionKey = partitionKey
                }

            Kin.PutRecordsResponse{..} ← runKinesis _pkKinesisKit Kin.PutRecords
              { Kin.putRecordsRecords = requestEntries
              , Kin.putRecordsStreamName = _pkStreamName
              }
            let
              processResult m m'
                | isJust (Kin.putRecordsResponseRecordErrorCode m') = Just m
                | otherwise = Nothing
            return ∘ catMaybes $ zipWith processResult eligibleItems putRecordsResponseRecords

    forM_ leftovers $ \items →
      unless (null items) $
        leftover $ items
          <&> mqiRemainingAttempts -~ 1
           & filter messageQueueItemIsEligible

-- | Enqueues a message to Kinesis on the next shard. If a message cannot be
-- enqueued, an error of type 'WriteProducerException' will be returned.
--
writeProducer
  ∷ MonadIO m
  ⇒ KinesisProducer
  → Message
  → m (Either WriteProducerException ())
writeProducer KinesisProducer{..} !msg =
  runExceptT $ do
    when (T.length msg > MaxMessageSize) $
      throwE MessageTooLarge

    gen ← liftIO R.newStdGen
    result ← liftIO $
      tryWriteQueue _kpMessageQueue MessageQueueItem
        { _mqiMessage = msg
        , _mqiPartitionKey = generatePartitionKey gen
        , _mqiRemainingAttempts = _kpRetryPolicy ^. rpRetryCount ∘ to succ
        }
    case result of
      Just written → unless written $ throwE ProducerQueueFull
      Nothing → throwE $ ProducerQueueClosed

-- | This constructs a 'KinesisProducer' and closes it when you have done with
-- it. This is equivalent to 'withKinesisProducer', but replaces the
-- continuation with a return in 'Codensity'.
--
managedKinesisProducer
  ∷ ( MonadIO m
    , MonadBaseControl IO m
    )
  ⇒ ProducerKit
  → Codensity m KinesisProducer
managedKinesisProducer kit@ProducerKit{_pkQueueImplementation = QueueImplementation (Proxy ∷ Proxy q)} = do
  messageQueue ← liftIO ∘ newQueue ∘ fromIntegral $ kit ^. pkMessageQueueBounds

  let
    producer = KinesisProducer
      { _kpMessageQueue = (messageQueue ∷ q)
      , _kpRetryPolicy = kit ^. pkRetryPolicy
      }

    chunkingPolicy = ChunkingPolicy
      { _cpMaxChunkSize = (kit ^. pkBatchPolicy ∘ bpBatchSize) * (kit ^. pkMaxConcurrency)
      , _cpMinChunkingInterval = 5000000
      }

    processQueue =
      chunkedSourceFromQueue chunkingPolicy messageQueue
        $$ putRecordsSink kit

    -- TODO: figure out better error handling here (such as a limit to respawns)
    workerLoop ∷ IO () = do
      result ← tryAny processQueue
      case result of
        Left exn → do
          hPutStrLn stderr $ "Respawning Kinesis producer worker loop after exception: " ++ show exn
          workerLoop
        Right () → return ()

    cleanupWorker _ = do
      closeQueue messageQueue
      withAsync processQueue $ \cleanupHandle → do
        case _pkCleanupTimeout kit of
          Just timeout →
            withAsync (threadDelay ∘ fromIntegral $ 1000 * timeout) $ \timeoutHandle → do
              result ← waitEitherCatchCancel timeoutHandle cleanupHandle
              case result of
                Left _timeoutResult →
                  throwIO ProducerCleanupTimedOut
                Right workerResult →
                  throwIO ∘ ProducerWorkerDied $ workerResult ^? _Left
          Nothing →
            wait cleanupHandle


  workerHandle ← Codensity $ bracket (async (liftIO workerLoop)) (liftIO ∘ cleanupWorker)

  Codensity $ \inner → do
    withAsync (inner producer) $ \innerHandle → do
      result ← waitEitherCatchCancel innerHandle workerHandle
      case result of
        Left innerResult →
          either throwIO return innerResult
        Right (workerResult ∷ Either SomeException ()) →
          throwIO ∘ ProducerWorkerDied $ workerResult ^? _Left

defaultQueueImplementation ∷ QueueImplementation
defaultQueueImplementation = QueueImplementation (Proxy ∷ ∀ α. Proxy (TBMChan α))

-- | This constructs a 'KinesisProducer' and closes it when you have done with
-- it.
--
withKinesisProducer
  ∷ ( MonadIO m
    , MonadBaseControl IO m
    )
  ⇒ ProducerKit
  → (KinesisProducer → m α)
  → m α
withKinesisProducer =
  runCodensity ∘ managedKinesisProducer

-- | map at most n actions concurrently
--
mapConcurrentlyN
  ∷ Traversable t
  ⇒ Natural -- ^ number of concurrent actions
  → Natural -- ^ startup delay between actions in milliseconds
  → (a → IO b)
  → t a
  → IO (t b)
mapConcurrentlyN n delay f t = do
  sem ← liftIO ∘ newQSem $ fromIntegral n
  mapConcurrently (run sem) t_
    where
      (_, t_) = mapAccumL (\i v → (succ i, (i,v))) 0 t
      run sem (i,a) =
        liftBaseOp_ (bracket_ (waitQSem sem) (signalQSem sem)) $ do
          liftIO ∘ threadDelay ∘ fromIntegral $ 1000 * delay * i
          f a

