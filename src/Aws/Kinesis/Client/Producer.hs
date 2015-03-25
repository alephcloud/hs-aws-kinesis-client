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
, pkKinesisKit
, pkStreamName
, pkBatchPolicy
, pkRetryPolicy
, pkMessageQueueBounds
, pkMaxConcurrency

-- * Exceptions
, WriteProducerException(..)
, ProducerCleanupTimedOut(..)
, ProducerWorkerDied(..)
, InvalidProducerKit(..)

, pattern MaxMessageSize

-- * Policies
, BatchPolicy
, defaultBatchPolicy
, bpBatchSize
, bpEndpoint

, RetryPolicy
, defaultRetryPolicy
, rpRetryCount

, RecordEndpoint(..)
) where

import qualified Aws.Kinesis as Kin
import Aws.Kinesis.Client.Common

import Control.Applicative
import Control.Concurrent.Async.Lifted
import Control.Concurrent.Lifted hiding (yield)
import Control.Concurrent.STM
import Control.Concurrent.STM.TBMQueue
import Control.Exception.Enclosed
import Control.Exception.Lifted
import Control.Lens
import Control.Monad
import Control.Monad.Codensity
import Control.Monad.Reader
import Control.Monad.Trans.Control
import Control.Monad.Trans.Except
import Data.Conduit
import qualified Data.Conduit.List as CL
import Data.Maybe
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Traversable
import Data.Typeable
import Prelude.Unicode
import qualified System.Random as R
import System.IO

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

-- | The default batching policy sends @200@ records per 'PutRecordsEndpoint'
-- request.
--
defaultBatchPolicy ∷ BatchPolicy
defaultBatchPolicy = BatchPolicy
  { _bpBatchSize = 200
  , _bpEndpoint = PutRecordsEndpoint
  }

-- | The producer will attempt to re-send records which failed according to a
-- user-specified policy. This policy applies to failures which occur in the
-- process of sending a message to Kinesis, not those which occur in the course
-- of enqueuing a message.
data RetryPolicy
  = RetryPolicy
  { _rpRetryCount ∷ {-# UNPACK #-} !Int
  } deriving (Eq, Show)

-- | The number of times to retry sending a message after it has first failed.
--
rpRetryCount ∷ Lens' RetryPolicy Int
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

  , _mqiRemainingAttempts ∷ !Int
  -- ^ The number of times remaining to try and publish this message
  } deriving (Eq, Show)

mqiMessage ∷ Lens' MessageQueueItem Message
mqiMessage = lens _mqiMessage $ \i m → i { _mqiMessage = m }

mqiPartitionKey ∷ Lens' MessageQueueItem Kin.PartitionKey
mqiPartitionKey = lens _mqiPartitionKey $ \i s → i { _mqiPartitionKey = s }

mqiRemainingAttempts ∷ Lens' MessageQueueItem Int
mqiRemainingAttempts = lens _mqiRemainingAttempts $ \i n → i { _mqiRemainingAttempts = n }

messageQueueItemIsEligible
  ∷ MessageQueueItem
  → Bool
messageQueueItemIsEligible =
  (≥ 1) ∘ _mqiRemainingAttempts

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

  , _pkMessageQueueBounds ∷ {-# UNPACK #-} !Int
  -- ^ The maximum number of records that may be enqueued at one time.

  , _pkMaxConcurrency ∷ {-# UNPACK #-} !Int
  -- ^ The number of requests to run concurrently (minimum: 1).

  , _pkCleanupTimeout ∷ !(Maybe Int)
  -- ^ The timeout in milliseconds, after which the producer's cleanup routine
  -- will terminate, finished or not, throwing 'ProducerCleanupTimedOut'.
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
pkMessageQueueBounds ∷ Lens' ProducerKit Int
pkMessageQueueBounds = lens _pkMessageQueueBounds $ \pk qb → pk { _pkMessageQueueBounds = qb }

-- | A lens for '_pkMaxConcurrency'.
--
pkMaxConcurrency ∷ Lens' ProducerKit Int
pkMaxConcurrency = lens _pkMaxConcurrency $ \pk n → pk { _pkMaxConcurrency = n }

-- | The (abstract) Kinesis producer client.
--
data KinesisProducer
  = KinesisProducer
  { _kpMessageQueue ∷ !(TBMQueue MessageQueueItem)
  , _kpRetryPolicy ∷ !RetryPolicy
  }

kpMessageQueue ∷ Getter KinesisProducer (TBMQueue MessageQueueItem)
kpMessageQueue = to _kpMessageQueue

kpRetryPolicy ∷ Getter KinesisProducer RetryPolicy
kpRetryPolicy = to _kpRetryPolicy

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

data InvalidProducerKit
  = InvalidConcurrentConsumerCount
  -- ^ Thrown when 'pkMaxConcurrency' is set with an invalid value.
  deriving (Typeable, Show, Eq)

instance Exception InvalidProducerKit

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

-- | This will take up to @n@ items from a 'TBMQueue', with a timeout.
--
takeTBMQueueWithTimeout
  ∷ ∀ α
  . Int -- ^ number of items to get
  → Int -- ^ timeout in microseconds
  → TBMQueue α
  → IO [α]
takeTBMQueueWithTimeout n timeout q = do
  timedOutVar ← newEmptyTMVarIO

  let
    timeoutAction = do
      threadDelay timeout
      atomically $ putTMVar timedOutVar ()

    go xs
      | length xs ≥ n = return xs
      | otherwise = do
          res ← Left <$> readTBMQueue q <|> Right <$> readTMVar timedOutVar
          case res of
            Left mx → maybe (return xs) (go ∘ (:xs)) mx
            Right () → return xs

  withAsync timeoutAction $ \h → do
    xs ← atomically $ go []
    xs <$ cancel h

-- | A policy for chunking the contents of the message queue.
--
data ChunkingPolicy
  = ChunkingPolicy
  { _cpMaxChunkSize ∷ !Int
  -- ^ The largest chunk size that is permitted.

  , _cpMinChunkingInterval ∷ !Int
  -- ^ The time in microseconds after which a chunk should be committed, even
  -- if the maximum chunk size has not yet been reached.
  }

-- | A 'Source' that reads chunks off a bounded STM queue according some
-- 'ChunkingPolicy'.
--
chunkedSourceTBMQueue
  ∷ ChunkingPolicy
  → TBMQueue α
  → Source IO [α]
chunkedSourceTBMQueue cp@ChunkingPolicy{..} q = do
  shouldTerminate ←
    liftIO ∘ atomically $
      (&&) <$> isClosedTBMQueue q <*> isEmptyTBMQueue q

  unless shouldTerminate $ do
    items ← lift $ takeTBMQueueWithTimeout _cpMaxChunkSize _cpMinChunkingInterval q
    unless (null items) $ do
      yield items

    chunkedSourceTBMQueue cp q

-- | A conduit for concurently sending multiple records to Kinesis using the
-- @PutRecord@ endpoint.
--
concurrentPutRecordSink
  ∷ ProducerKit
  → Sink [MessageQueueItem] IO ()
concurrentPutRecordSink kit@ProducerKit{..} = do
  awaitForever $ \messages → do
    lift ∘ flip (mapConcurrentlyN _pkMaxConcurrency 100) messages $ \m → do
      CL.sourceList [m] $$ putRecordSink kit


-- | A conduit for sending a record to Kinesis using the @PutRecord@ endpoint;
-- this is a conduit in order to restore failed messages as leftovers.
--
putRecordSink
  ∷ ProducerKit
  → Sink MessageQueueItem IO ()
putRecordSink ProducerKit{..} = do
  awaitForever $ \item → do
    when (messageQueueItemIsEligible item) $ do
      let partitionKey = item ^. mqiPartitionKey
      result ← lift ∘ tryAny $ runKinesis _pkKinesisKit Kin.PutRecord
        { Kin.putRecordData = item ^. mqiMessage ∘ to T.encodeUtf8
        , Kin.putRecordExplicitHashKey = Nothing
        , Kin.putRecordPartitionKey = partitionKey
        , Kin.putRecordSequenceNumberForOrdering = Nothing
        , Kin.putRecordStreamName = _pkStreamName
        }
      case result of
        Left (SomeException e) → do
          liftIO $ do
            hPutStrLn stderr $ "Kinesis producer client error (will wait 5s): " ++ show e
            threadDelay 5000000
          leftover $ item & mqiRemainingAttempts -~ 1
        Right _ → return ()

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

sendMessagesSink
  ∷ ProducerKit
  → Sink [MessageQueueItem] IO ()
sendMessagesSink kit@ProducerKit{..} = do
  case _pkBatchPolicy ^. bpEndpoint of
    PutRecordsEndpoint → putRecordsSink kit
    PutRecordEndpoint → concurrentPutRecordSink kit

-- | Enqueues a message to Kinesis on the next shard. If a message cannot be
-- enqueued, an error of type 'WriteProducerException' will be returned.
--
writeProducer
  ∷ MonadIO m
  ⇒ KinesisProducer
  → Message
  → m (Either WriteProducerException ())
writeProducer producer !msg =
  runExceptT $ do
    when (T.length msg > MaxMessageSize) $
      throwE MessageTooLarge

    gen ← liftIO R.newStdGen
    result ← liftIO ∘ atomically $ do
      tryWriteTBMQueue (producer ^. kpMessageQueue) MessageQueueItem
        { _mqiMessage = msg
        , _mqiPartitionKey = generatePartitionKey gen
        , _mqiRemainingAttempts = producer ^. kpRetryPolicy . rpRetryCount . to succ
        }
    case result of
      Just written → unless written $ throwE ProducerQueueFull
      Nothing → throwE $ ProducerQueueClosed

-- | This constructs a 'KinesisProducer' and closes it when you have done with
-- it. This is equivalent to 'withKinesisProducer', but replaces the
-- continuation with a return in 'Codensity'.
--
managedKinesisProducer
  ∷ ∀ m
  . ( MonadIO m
    , MonadBaseControl IO m
    )
  ⇒ ProducerKit
  → Codensity m KinesisProducer
managedKinesisProducer kit = do
  when (kit ^. pkMaxConcurrency < 1) ∘ lift $
    throw InvalidConcurrentConsumerCount

  messageQueue ← liftIO ∘ newTBMQueueIO $ kit ^. pkMessageQueueBounds

  let
    producer = KinesisProducer
      { _kpMessageQueue = messageQueue
      , _kpRetryPolicy = kit ^. pkRetryPolicy
      }

    chunkingPolicy = ChunkingPolicy
      { _cpMaxChunkSize = (kit ^. pkBatchPolicy ∘ bpBatchSize) * (kit ^. pkMaxConcurrency)
      , _cpMinChunkingInterval = 5000000
      }

    -- TODO: figure out better error handling here (such as a limit to respawns)
    workerLoop ∷ IO () = do
      result ← tryAny $
        chunkedSourceTBMQueue chunkingPolicy messageQueue
          $$ sendMessagesSink kit
      case result of
        Left exn → do
          hPutStrLn stderr $ "Respawning Kinesis producer worker loop after exception: " ++ show exn
          workerLoop
        Right () → return ()

    cleanupWorker _ = do
      atomically $ closeTBMQueue messageQueue

  workerHandle ← Codensity $ bracket (async (liftIO workerLoop)) (liftIO ∘ cleanupWorker)

  Codensity $ \inner → do
    withAsync (inner producer) $ \innerHandle → do
      result ← waitEitherCatch innerHandle workerHandle
      case result of
        Left innerResult → do
          liftIO $ atomically $ closeTBMQueue messageQueue
          case _pkCleanupTimeout kit of
            Just timeout →
              withAsync (threadDelay $ 1000 * timeout) $ \timeoutHandle → do
                result' ← waitEitherCatchCancel timeoutHandle workerHandle
                case result' of
                  Left (_timeoutResult ∷ Either SomeException ()) →
                    throw ProducerCleanupTimedOut
                  Right (workerResult ∷ Either SomeException ()) →
                    throw ∘ ProducerWorkerDied $ workerResult ^? _Left
            Nothing → do
              liftIO $ atomically $ closeTBMQueue messageQueue
              wait workerHandle ∷ m ()

          either throw return innerResult

        Right (workerResult ∷ Either SomeException ()) →
          throw ∘ ProducerWorkerDied $ workerResult ^? _Left

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
  ⇒ Int -- ^ number of concurrent actions
  → Int -- ^ startup delay between actions in milliseconds
  → (a → IO b)
  → t a
  → IO (t b)
mapConcurrentlyN n delay f t = do
  sem ← liftIO $ newQSem n
  mapConcurrently (run sem) t_
    where
      (_, t_) = mapAccumL (\i v → (succ i, (i,v))) 0 t
      run sem (i,a) =
        liftBaseOp_ (bracket_ (waitQSem sem) (signalQSem sem)) $ do
          liftIO ∘ threadDelay $ 1000 * delay * i
          f a

