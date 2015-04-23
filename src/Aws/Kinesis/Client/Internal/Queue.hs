-- Copyright (c) 2013-2014 PivotCloud, Inc.
--
-- Aws.Kinesis.Client.Internal.Queue
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

{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE UnicodeSyntax #-}

-- |
-- Module: Aws.Kinesis.Client.Internal.Queue
-- Copyright: Copyright © 2013-2014 PivotCloud, Inc.
-- License: Apache-2.0
-- Maintainer: Jon Sterling <jsterling@alephcloud.com>
-- Stability: experimental
--
module Aws.Kinesis.Client.Internal.Queue
( BoundedCloseableQueue(..)
) where

import Control.Applicative
import Control.Concurrent.STM
import Control.Concurrent.STM.TBMQueue
import Control.Concurrent.STM.TBMChan
import Control.Monad.Unicode
import Numeric.Natural
import Prelude.Unicode

-- | A signature for bounded, closeable queues.
--
class BoundedCloseableQueue q α | q → α where
  -- Create a queue with size @n@.
  newQueue
    ∷ Natural -- ^ the size @n@ of the queue
    → IO q

  -- Close the queue.
  closeQueue
    ∷ q
    → IO ()

  -- | Returns 'False' if and only if the queue is closed. If the queue is full
  -- this function shall block.
  writeQueue
    ∷ q
    → α
    → IO Bool

  -- | Non-blocking version of 'writeQueue'. Returns 'Nothing' if the queue was
  -- full. Otherwise it returns 'Just True' if the value was successfully
  -- written and 'Just False' if the queue was closed.
  tryWriteQueue
    ∷ q
    → α
    → IO (Maybe Bool)

  -- | Returns 'Nothing' if and only if the queue is closed. If this queue is
  -- empty this function blocks.
  readQueue
    ∷ q
    → IO (Maybe α)

  -- | Take up to @n@ items from the queue with a timeout of @t@.
  takeQueueTimeout
    ∷ q
    → Natural -- ^ the number of items @n@ to take
    → Natural -- ^ the timeout @t@ in microseconds
    → IO [α]

  -- | Whether the queue is empty.
  isEmptyQueue
    ∷ q
    → IO Bool

  -- | Whether the queue is closed.
  isClosedQueue
    ∷ q
    → IO Bool

  -- | Whether the queue is empty and closed. The trivial default
  -- implementation may be overridden with one which provides transactional
  -- guarantees.
  isClosedAndEmptyQueue
    ∷ q
    → IO Bool
  isClosedAndEmptyQueue q =
    (&&) <$> isEmptyQueue q <*> isClosedQueue q

instance BoundedCloseableQueue (TBMQueue a) a where
  newQueue =
    newTBMQueueIO ∘ fromIntegral

  closeQueue =
    atomically ∘ closeTBMQueue

  writeQueue q a =
    atomically $ isClosedTBMQueue q ≫= \case
      True → return False
      False → True <$ writeTBMQueue q a

  tryWriteQueue q a =
    atomically $ tryWriteTBMQueue q a ≫= \case
      Nothing → return $ Just False
      Just False → return Nothing
      Just True → return $ Just True

  readQueue =
    atomically ∘ readTBMQueue

  takeQueueTimeout q n timeoutDelay = do
    timedOutVar ← registerDelay $ fromIntegral timeoutDelay

    let
      timeout =
        readTVar timedOutVar ≫= check

      readItems xs = do
        atomically (Left <$> timeout <|> Right <$> readTBMQueue q) ≫= \case
          Left _ → return xs
          Right Nothing → return xs
          Right (Just x) → go (x:xs)

      go xs
        | length xs ≥ fromIntegral n = return xs
        | otherwise = readItems xs

    go []

  isClosedQueue =
    atomically ∘ isClosedTBMQueue

  isEmptyQueue =
    atomically ∘ isEmptyTBMQueue

  isClosedAndEmptyQueue q =
    atomically $
      (&&) <$> isClosedTBMQueue q <*> isEmptyTBMQueue q

instance BoundedCloseableQueue (TBMChan a) a where
  newQueue =
    newTBMChanIO ∘ fromIntegral

  closeQueue =
    atomically ∘ closeTBMChan

  writeQueue q a =
    atomically $ isClosedTBMChan q ≫= \case
      True → return False
      False → True <$ writeTBMChan q a

  tryWriteQueue q a =
    atomically $ tryWriteTBMChan q a ≫= \case
      Nothing → return $ Just False
      Just False → return Nothing
      Just True → return $ Just True

  readQueue =
    atomically ∘ readTBMChan

  isClosedQueue =
    atomically ∘ isClosedTBMChan

  isEmptyQueue =
    atomically ∘ isEmptyTBMChan

  isClosedAndEmptyQueue q =
    atomically $
      (&&) <$> isClosedTBMChan q <*> isEmptyTBMChan q

  takeQueueTimeout q n timeoutDelay = do
    timedOutVar ← registerDelay $ fromIntegral timeoutDelay

    let
      timeout =
        readTVar timedOutVar ≫= check

      readItems xs = do
        atomically (Left <$> timeout <|> Right <$> readTBMChan q) ≫= \case
          Left _ → return xs
          Right Nothing → return xs
          Right (Just x) → go (x:xs)

      go xs
        | length xs ≥ fromIntegral n = return xs
        | otherwise = readItems xs

    go []

