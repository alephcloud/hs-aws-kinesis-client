-- Copyright (c) 2013-2015 PivotCloud, Inc.
--
-- Aws.Kinesis.Client.Consumer
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

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE UnicodeSyntax #-}

-- |
-- Module: CLI.Record
-- Copyright: Copyright © 2013-2015 PivotCloud, Inc.
-- License: Apache-2.0
-- Maintainer: Jon Sterling <jsterling@alephcloud.com>
-- Stability: experimental
--

module CLI.Record where

import Data.Aeson
import Data.Aeson.Types
import Data.Time
import qualified Data.Text as T
import qualified Data.ByteString as BS
import Control.Applicative
import Control.Applicative.Unicode
import Data.Hourglass
import Prelude.Unicode

data Record
  = Record
  { rTimestamp ∷ DateTime
  , rMessage ∷ T.Text
  } deriving (Show, Eq)

instance FromJSON DateTime where
  parseJSON =
    withText "DateTime" $
      maybe (fail "Invalid DateTime") return
      ∘ timeParse fmt
      ∘ T.unpack
    where
      fmt =
        [ Format_Year4
        , hyphen
        , Format_Month2
        , hyphen
        , Format_Day2
        , Format_Text 'T'
        , Format_Hour
        , colon
        , Format_Minute
        , colon
        , Format_Second
        ]
      hyphen = Format_Text '-'
      colon = Format_Text ':'

instance FromJSON Record where
  parseJSON =
    withObject "Record" $ \xs →
      Record <$> xs .: "time" ⊛ xs .: "message"

instance Ord Record where
  compare r1 r2 = compare (rTimestamp r1) (rTimestamp r2)

