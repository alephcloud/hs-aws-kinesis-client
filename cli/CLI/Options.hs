-- Copyright (c) 2013-2014 PivotCloud, Inc.
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
-- Module: CLI.Options
-- Copyright: Copyright © 2013-2014 PivotCloud, Inc.
-- License: Apache-2.0
-- Maintainer: Jon Sterling <jsterling@alephcloud.com>
-- Stability: experimental
--

module CLI.Options
( CLIOptions(..)
, optionsParser
, parserInfo
) where

import Aws.Kinesis
import Control.Applicative.Unicode
import Control.Lens
import Control.Monad.Unicode
import Data.Monoid.Unicode
import qualified Data.Text as T
import qualified Data.Text.Lens as T
import Data.Hourglass
import Options.Applicative
import Prelude.Unicode

data CLIOptions
  = CLIOptions
  { clioStreamName ∷ StreamName
  , clioLimit ∷ Int
  , clioIteratorType ∷ ShardIteratorType
  , clioStartDate ∷ Maybe DateTime
  , clioEndDate ∷ Maybe DateTime
  -- , clioTimeDuration ∷ Maybe Seconds
  , clioRaw ∷ Bool
  } deriving Show

eitherTextReader
  ∷ ( T.IsText i
    , T.IsText e
    )
  ⇒ (i → Either e a)
  → ReadM a
eitherTextReader f =
  eitherReader $
    (_Left %~ view T.unpacked) ∘ f ∘ view T.packed

streamNameParser ∷ Parser StreamName
streamNameParser =
  option (eitherTextReader streamName) $
    long "stream-name"
    ⊕ short 's'
    ⊕ metavar "SN"
    ⊕ help "Fetch from the Kinesis stream named `SN`"

limitParser ∷ Parser Int
limitParser =
  option auto $
    long "limit"
    ⊕ short 'l'
    ⊕ metavar "L"
    ⊕ help "Fetch `L` records"

iteratorTypeParser ∷ Parser ShardIteratorType
iteratorTypeParser =
  option auto $
    long "iterator-type"
    ⊕ short 'i'
    ⊕ metavar "IT"
    ⊕ help "Iterator type (Latest|TrimHorizon)"
    ⊕ value TrimHorizon
    ⊕ showDefault

readDateTime
  ∷ String
  → ReadM DateTime
readDateTime =
  maybe (readerError "Invalid DateTime") return
  ∘ timeParse ISO8601_DateAndTime

startDateParser ∷ Parser DateTime
startDateParser =
  option (str ≫= readDateTime) $
    long "start-date"
    ⊕ short 's'
    ⊕ metavar "SD"
    ⊕ help "Start Date (ISO 8601)"

endDateParser ∷ Parser DateTime
endDateParser =
  option (str ≫= readDateTime) $
    long "end-date"
    ⊕ short 'e'
    ⊕ metavar "ED"
    ⊕ help "End Date (ISO 8601)"

timeDurationParser ∷ Parser Seconds
timeDurationParser =
  option auto $
    long "end-date"
    ⊕ short 'e'
    ⊕ metavar "ED"
    ⊕ help "Time window from start (in seconds)"

optionsParser ∷ Parser CLIOptions
optionsParser =
  CLIOptions
    <$> streamNameParser
    ⊛ limitParser
    ⊛ iteratorTypeParser
    ⊛ optional startDateParser
    ⊛ optional endDateParser
    -- ⊛ optional timeDurationParser
    ⊛ switch (long "raw" ⊕ help "Treat records as raw text")

parserInfo ∷ ParserInfo CLIOptions
parserInfo =
  info (helper ⊛ optionsParser) $
    fullDesc
    ⊕ progDesc "Fetch `L` records from a Kinesis stream `SN`. Put your AWS keys in ~/.aws-keys"
    ⊕ header "The Kinesis Consumer CLI"

