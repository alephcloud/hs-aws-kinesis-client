-- Copyright (c) 2013-2015 PivotCloud, Inc.
--
-- Aws.Kinesis.Client.Consumer.Internal.Kit
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

{-# LANGUAGE UnicodeSyntax #-}

-- |
-- Module: Aws.Kinesis.Client.Consumer.Internal.Kit
-- Copyright: Copyright © 2013-2015 PivotCloud, Inc.
-- License: Apache-2.0
-- Maintainer: Jon Sterling <jsterling@alephcloud.com>
-- Stability: experimental
--
module Aws.Kinesis.Client.Consumer.Internal.Kit
( ConsumerKit(..)
, makeConsumerKit
, ckKinesisKit
, ckStreamName
, ckBatchSize
, ckIteratorType
, ckSavedStreamState
) where

import Aws.Kinesis
import Aws.Kinesis.Client.Common
import Aws.Kinesis.Client.Consumer.Internal.SavedStreamState
import Control.Lens
import Numeric.Natural

-- | The 'ConsumerKit' contains what is needed to initialize a 'KinesisConsumer'.
data ConsumerKit
  = ConsumerKit
  { _ckKinesisKit ∷ !KinesisKit
  -- ^ The credentials and configuration for making requests to AWS Kinesis.

  , _ckStreamName ∷ !StreamName
  -- ^ The name of the stream to consume from.

  , _ckBatchSize ∷ !Natural
  -- ^ The number of records to fetch at once from the stream.

  , _ckIteratorType ∷ !ShardIteratorType
  -- ^ The type of iterator to consume.

  , _ckSavedStreamState ∷ !(Maybe SavedStreamState)
  -- ^ Optionally, an initial stream state. The iterator type in
  -- '_ckIteratorType' will be used for any shards not present in the saved
  -- stream state; otherwise, 'AfterSequenceNumber' will be used.
  }

-- | Create a 'ConsumerKit' with default settings (using iterator type
-- 'Latest' and a batch size of @200@).
--
makeConsumerKit
  ∷ KinesisKit
  → StreamName
  → ConsumerKit
makeConsumerKit kinesisKit stream = ConsumerKit
  { _ckKinesisKit = kinesisKit
  , _ckStreamName = stream
  , _ckBatchSize = 200
  , _ckIteratorType = Latest
  , _ckSavedStreamState = Nothing
  }

-- | A lens for '_ckKinesisKit'.
--
ckKinesisKit ∷ Lens' ConsumerKit KinesisKit
ckKinesisKit = lens _ckKinesisKit $ \ck kk → ck { _ckKinesisKit = kk }

-- | A lens for '_ckStreamName'.
--
ckStreamName ∷ Lens' ConsumerKit StreamName
ckStreamName = lens _ckStreamName $ \ck sn → ck { _ckStreamName = sn }

-- | A lens for '_ckBatchSize'.
--
ckBatchSize ∷ Lens' ConsumerKit Natural
ckBatchSize = lens _ckBatchSize $ \ck bs → ck { _ckBatchSize = bs }

-- | A lens for '_ckIteratorType'.
--
ckIteratorType ∷ Lens' ConsumerKit ShardIteratorType
ckIteratorType = lens _ckIteratorType $ \ck it → ck { _ckIteratorType = it }

-- | A lens for '_ckSavedStreamState'.
--
ckSavedStreamState ∷ Lens' ConsumerKit (Maybe SavedStreamState)
ckSavedStreamState = lens _ckSavedStreamState $ \ck ss → ck { _ckSavedStreamState = ss }

