-- Copyright (c) 2013-2015 PivotCloud, Inc.
--
-- Aws.Kinesis.Client.Internal.Queue.Chunk
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

{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE UnicodeSyntax #-}

-- |
-- Module: Aws.Kinesis.Client.Internal.Queue.Chunk
-- Copyright: Copyright © 2013-2015 PivotCloud, Inc.
-- License: Apache-2.0
-- Maintainer: Jon Sterling <jsterling@alephcloud.com>
-- Stability: experimental
--
module Aws.Kinesis.Client.Internal.Queue.Chunk
( ChunkingPolicy(..)
, chunkedSourceFromQueue
) where

import Aws.Kinesis.Client.Internal.Queue

import Control.Monad
import Control.Monad.Trans
import Data.Conduit
import Numeric.Natural

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
  shouldTerminate ← lift $ isClosedAndEmptyQueue q
  unless shouldTerminate $ do
    items ← lift $ takeQueueTimeout q _cpMaxChunkSize _cpMinChunkingInterval
    unless (null items) $ do
      yield items

    chunkedSourceFromQueue cp q

