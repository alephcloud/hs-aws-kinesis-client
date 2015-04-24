-- Copyright (c) 2013-2015 PivotCloud, Inc.
--
-- Aws.Kinesis.Client.Producer.Kit
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

{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE UnicodeSyntax #-}

-- |
-- Module: Aws.Kinesis.Client.Producer.Kit
-- Copyright: Copyright © 2013-2015 PivotCloud, Inc.
-- License: Apache-2.0
-- Maintainer: Jon Sterling <jsterling@alephcloud.com>
-- Stability: experimental
--
module Aws.Kinesis.Client.Producer.Kit
( -- * Producer Kit
  ProducerKit
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

  -- ** Constants
, pattern MaxMessageSize
) where

import Aws.Kinesis.Client.Producer.Internal

-- | The maximum size in bytes of a message.
--
pattern MaxMessageSize = 51000

