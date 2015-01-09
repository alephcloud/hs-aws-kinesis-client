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
{-# LANGUAGE TemplateHaskell #-}
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
, clioStreamName
, clioLimit
, clioIteratorType
, optionsParser
, parserInfo
) where

import Aws.Kinesis
import Control.Applicative.Unicode
import Control.Lens
import Data.Monoid.Unicode
import qualified Data.Text.Lens as T
import Options.Applicative
import Prelude.Unicode

data CLIOptions
  = CLIOptions
  { _clioStreamName ∷ !StreamName
  , _clioLimit ∷ !Int
  , _clioIteratorType ∷ !ShardIteratorType
  } deriving Show

makeLenses ''CLIOptions

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

optionsParser ∷ Parser CLIOptions
optionsParser =
  pure CLIOptions
    ⊛ streamNameParser
    ⊛ limitParser
    ⊛ iteratorTypeParser

parserInfo ∷ ParserInfo CLIOptions
parserInfo =
  info (helper ⊛ optionsParser) $
    fullDesc
    ⊕ progDesc "Fetch `L` records from a Kinesis stream `SN`. Put your AWS keys in ~/.aws-keys"
    ⊕ header "The Kinesis Consumer CLI"

