-- Copyright (c) 2013-2014 PivotCloud, Inc.
--
-- Aws.Kinesis.Client.Consumer.Internal.ShardState
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
-- Module: Aws.Kinesis.Client.Consumer.Internal.ShardState
-- Copyright: Copyright © 2013-2014 PivotCloud, Inc.
-- License: Apache-2.0
-- Maintainer: Jon Sterling <jsterling@alephcloud.com>
-- Stability: experimental
--
module Aws.Kinesis.Client.Consumer.Internal.ShardState
( ShardState
, makeShardState
, ssIterator
, ssShardId
, ssLastSequenceNumber
) where

import Aws.Kinesis

import Control.Concurrent.STM.TVar
import Control.Lens
import Prelude.Unicode

-- | The internal representation for shards used by the consumer.
--
data ShardState
  = ShardState
  { _ssIterator ∷ !(TVar (Maybe ShardIterator))
  , _ssShardId ∷ !ShardId
  , _ssLastSequenceNumber ∷ !(TVar (Maybe SequenceNumber))
  }

makeShardState
  ∷ ShardId
  → TVar (Maybe ShardIterator)
  → TVar (Maybe SequenceNumber)
  → ShardState
makeShardState sid vit vlsn = do
  ShardState
    { _ssIterator = vit
    , _ssShardId = sid
    , _ssLastSequenceNumber = vlsn
    }

-- | A getter for '_ssIterator'.
--
ssIterator ∷ Getter ShardState (TVar (Maybe ShardIterator))
ssIterator = to _ssIterator

-- | A lens for '_ssShardId'.
--
ssShardId ∷ Lens' ShardState ShardId
ssShardId = lens _ssShardId $ \ss sid → ss { _ssShardId = sid }

-- | A getter for '_ssLastSequenceNumber'.
--
ssLastSequenceNumber ∷ Getter ShardState (TVar (Maybe SequenceNumber))
ssLastSequenceNumber = to _ssLastSequenceNumber

-- | 'ShardState' is quotiented by shard ID.
--
instance Eq ShardState where
  ss == ss' = ss ^. ssShardId ≡ ss' ^. ssShardId

