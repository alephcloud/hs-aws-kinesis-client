-- Copyright (c) 2013-2014 PivotCloud, Inc.
--
-- Aws.Kinesis.Client.Queue
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
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UnicodeSyntax #-}

-- |
-- Module: Aws.Kinesis.Client.Queue
-- Copyright: Copyright © 2013-2014 PivotCloud, Inc.
-- License: Apache-2.0
-- Maintainer: Jon Sterling <jsterling@alephcloud.com>
-- Stability: experimental
--
module Aws.Kinesis.Client.Queue
( Queue(..)
-- , BoundedQueue(..)
-- , CloseableQueue(..)
) where

import Control.Concurrent.STM
import Control.Concurrent.STM.TBMQueue
import Prelude.Unicode

-- | An abstract signature for queues.
--
class Queue m q | q → m where
  newQueue
    ∷ Int
    → m (q α)

  -- | Read a value from the queue (blocking).
  popQueue
    ∷ q α
    → m (Maybe α)

  -- | Write a value to the queue.
  pushQueue
    ∷ q α
    → α
    → m ()

  tryPushQueue
    ∷ q α
    → α
    → m (Maybe Bool)

  isFullQueue
    ∷ q α
    → m Bool

  closeQueue
    ∷ q α
    → m ()

  isClosedQueue
    ∷ q α
    → m Bool

  isEmptyQueue
    ∷ q α
    → m Bool

instance Queue STM TBMQueue where
  newQueue = newTBMQueue
  popQueue = readTBMQueue
  pushQueue = writeTBMQueue
  tryPushQueue = tryWriteTBMQueue
  closeQueue = closeTBMQueue
  isFullQueue = isFullTBMQueue
  isClosedQueue = isClosedTBMQueue
  isEmptyQueue = isEmptyTBMQueue

newtype IOQueue q α = IOQueue (q α)

instance Queue STM q ⇒ Queue IO (IOQueue q) where
  newQueue = fmap IOQueue ∘ atomically ∘ newQueue
  popQueue (IOQueue q) = atomically $ popQueue q
  pushQueue (IOQueue q) = atomically ∘ pushQueue q
  tryPushQueue (IOQueue q) = atomically ∘ tryPushQueue q
  closeQueue (IOQueue q) = atomically $ closeQueue q
  isFullQueue (IOQueue q) = atomically $ isFullQueue q
  isClosedQueue (IOQueue q) = atomically $ isClosedQueue q
  isEmptyQueue (IOQueue q) = atomically $ isEmptyQueue q
