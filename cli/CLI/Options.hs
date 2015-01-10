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
, clioAccessKeys
, clioPrintState
, AccessKeys(..)
, akAccessKeyId
, akSecretAccessKey
, optionsParser
, parserInfo
) where

import Aws.Kinesis
import qualified Data.ByteString.Char8 as B8
import Control.Applicative.Unicode
import Control.Lens
import Data.Monoid.Unicode
import qualified Data.Text.Lens as T
import Options.Applicative
import Prelude.Unicode

data AccessKeys
  = AccessKeys
  { _akAccessKeyId ∷ !B8.ByteString
  , _akSecretAccessKey ∷ !B8.ByteString
  } deriving Show

data CLIOptions
  = CLIOptions
  { _clioStreamName ∷ !StreamName
  , _clioLimit ∷ !Int
  , _clioIteratorType ∷ !ShardIteratorType
  , _clioAccessKeys ∷ !(Either AccessKeys FilePath)
  , _clioPrintState ∷ !Bool
  } deriving Show

makeLenses ''AccessKeys
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

accessKeyIdParser ∷ Parser B8.ByteString
accessKeyIdParser =
  fmap B8.pack ∘ strOption $
    long "access-key-id"
    ⊕ metavar "ID"
    ⊕ help "Your AWS access key id"

secretAccessKeyParser ∷ Parser B8.ByteString
secretAccessKeyParser =
  fmap B8.pack ∘ strOption $
    long "secret-access-key"
    ⊕ metavar "SK"
    ⊕ help "Your AWS secret access key"

accessKeysParser ∷ Parser AccessKeys
accessKeysParser =
  pure AccessKeys
    ⊛ accessKeyIdParser
    ⊛ secretAccessKeyParser

accessKeysPathParser ∷ Parser FilePath
accessKeysPathParser =
  strOption $
    long "access-keys-path"
    ⊕ metavar "PATH"
    ⊕ help "The path to a file containing your access keys. To be formatted \"default ID SECRET\""

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

printStateParser ∷ Parser Bool
printStateParser =
  option auto $
    long "print-state"
    ⊕ help "Whether to print the last read sequence number for each shard upon termination"
    ⊕ metavar "BOOL"
    ⊕ value False
    ⊕ showDefault

optionsParser ∷ Parser CLIOptions
optionsParser =
  pure CLIOptions
    ⊛ streamNameParser
    ⊛ limitParser
    ⊛ iteratorTypeParser
    ⊛ (Left <$> accessKeysParser <|> Right <$> accessKeysPathParser)
    ⊛ printStateParser

parserInfo ∷ ParserInfo CLIOptions
parserInfo =
  info (helper ⊛ optionsParser) $
    fullDesc
    ⊕ progDesc "Fetch `L` records from a Kinesis stream `SN`."
    ⊕ header "The Kinesis Consumer CLI"

