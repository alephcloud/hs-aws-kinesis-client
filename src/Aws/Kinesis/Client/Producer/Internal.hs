-- Copyright (c) 2013-2015 PivotCloud, Inc.
--
-- Aws.Kinesis.Client.Producer.Internal
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

{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE UnicodeSyntax #-}

-- |
-- Module: Aws.Kinesis.Client.Producer.Internal
-- Copyright: Copyright © 2013-2015 PivotCloud, Inc.
-- License: Apache-2.0
-- Maintainer: Jon Sterling <jsterling@alephcloud.com>
-- Stability: experimental
--
module Aws.Kinesis.Client.Producer.Internal
( ProducerKit(..)
, makeProducerKit
, pkKinesisKit
, pkStreamName
, pkBatchPolicy
, pkRetryPolicy
, pkMessageQueueBounds
, pkMaxConcurrency
, pkCleanupTimeout
, pkQueueImplementation

  -- * Queue Implementations
, QueueImplementation(..)
, defaultQueueImplementation

  -- * Policies
, BatchPolicy
, defaultBatchPolicy
, bpBatchSize

, RetryPolicy
, defaultRetryPolicy
, rpRetryCount

  -- * Other Types
, Message
, MessageQueueItem(..)
, mqiMessage
, mqiRemainingAttempts
, mqiPartitionKey
, messageQueueItemIsEligible
) where

import Aws.Kinesis
import Aws.Kinesis.Client.Common
import Aws.Kinesis.Client.Internal.Queue

import Control.Concurrent.STM.TBMChan
import Control.Lens
import Data.Proxy
import qualified Data.Text as T
import Numeric.Natural
import Prelude.Unicode

type Message = T.Text

data MessageQueueItem
  = MessageQueueItem
  { _mqiMessage ∷ !Message
  -- ^ The contents of the message

  , _mqiPartitionKey ∷ !PartitionKey
  -- ^ The partition key the message is destined for

  , _mqiRemainingAttempts ∷ !Natural
  -- ^ The number of times remaining to try and publish this message
  } deriving (Eq, Show)

mqiMessage ∷ Lens' MessageQueueItem Message
mqiMessage = lens _mqiMessage $ \i m → i { _mqiMessage = m }

mqiPartitionKey ∷ Lens' MessageQueueItem PartitionKey
mqiPartitionKey = lens _mqiPartitionKey $ \i s → i { _mqiPartitionKey = s }

mqiRemainingAttempts ∷ Lens' MessageQueueItem Natural
mqiRemainingAttempts = lens _mqiRemainingAttempts $ \i n → i { _mqiRemainingAttempts = n }

messageQueueItemIsEligible
  ∷ MessageQueueItem
  → Bool
messageQueueItemIsEligible =
  (≥ 1) ∘ _mqiRemainingAttempts

-- | The producer batches records according to a user-specified policy.
--
data BatchPolicy
  = BatchPolicy
  { _bpBatchSize ∷ !Natural
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
  { _rpRetryCount ∷ !Natural
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

-- | A proxy object for specifying a concrete queue implementation. You may
-- provide your own, or use 'defaultQueueImplementation'.
--
data QueueImplementation
  = ∀ proxy q
  . BoundedCloseableQueue q MessageQueueItem
  ⇒ QueueImplementation (proxy q)

-- | The default 'QueueImplementation' is based on 'TBMChan'.
--
defaultQueueImplementation ∷ QueueImplementation
defaultQueueImplementation = QueueImplementation (Proxy ∷ ∀ α. Proxy (TBMChan α))

-- | The basic input required to construct a Kinesis producer.
--
data ProducerKit
  = ProducerKit
  { _pkKinesisKit ∷ !KinesisKit
  -- ^ The basic information required to send requests to AWS Kinesis.

  , _pkStreamName ∷ !StreamName
  -- ^ The name of the stream to send records to.

  , _pkBatchPolicy ∷ !BatchPolicy
  -- ^ The record batching policy for the producer.

  , _pkRetryPolicy ∷ !RetryPolicy
  -- ^ The retry policy for the producer.

  , _pkMessageQueueBounds ∷ !Natural
  -- ^ The maximum number of records that may be enqueued at one time.

  , _pkMaxConcurrency ∷ !Natural
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
  → StreamName
  → ProducerKit
makeProducerKit kinesisKit stream = ProducerKit
  { _pkKinesisKit = kinesisKit
  , _pkStreamName = stream
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
pkStreamName ∷ Lens' ProducerKit StreamName
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
