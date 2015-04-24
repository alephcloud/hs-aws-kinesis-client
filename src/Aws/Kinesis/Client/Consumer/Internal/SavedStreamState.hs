-- Copyright (c) 2013-2015 PivotCloud, Inc.
--
-- Aws.Kinesis.Client.Consumer.Internal.SavedStreamState
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
-- Module: Aws.Kinesis.Client.Consumer.Internal.SavedStreamState
-- Copyright: Copyright © 2013-2015 PivotCloud, Inc.
-- License: Apache-2.0
-- Maintainer: Jon Sterling <jsterling@alephcloud.com>
-- Stability: experimental
--
module Aws.Kinesis.Client.Consumer.Internal.SavedStreamState
( SavedStreamState
, _SavedStreamState
) where

import Aws.Kinesis
import Control.Applicative (pure)
import Control.Applicative.Unicode
import Control.Lens hiding ((.=))
import Data.Aeson
import qualified Data.Map as M
import qualified Data.HashMap.Strict as HM
import Data.Traversable
import Prelude.Unicode

newtype SavedStreamState
  = SavedStreamState
  { _savedStreamState ∷ M.Map ShardId SequenceNumber
  }

-- | An iso for 'SavedStreamState'.
--
_SavedStreamState ∷ Iso' SavedStreamState (M.Map ShardId SequenceNumber)
_SavedStreamState = iso _savedStreamState SavedStreamState

instance ToJSON SavedStreamState where
  toJSON (SavedStreamState m) =
    Object ∘ HM.fromList ∘ flip fmap (M.toList m) $ \(sid, sn) →
      let String sid' = toJSON sid
      in sid' .= sn

instance FromJSON SavedStreamState where
  parseJSON =
    withObject "SavedStreamState" $ \xs → do
      fmap (SavedStreamState ∘ M.fromList) ∘ for (HM.toList xs) $ \(sid, sn) → do
        pure (,)
          ⊛ parseJSON (String sid)
          ⊛ parseJSON sn


