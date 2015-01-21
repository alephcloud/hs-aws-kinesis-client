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
, clioStateIn
, clioStateOut
, clioUseInstanceMetadata
, clioRegion
, CredentialsInput(..)
, AccessKeys(..)
, akAccessKeyId
, akSecretAccessKey
, optionsParser
, parserInfo
) where

import Aws.General
import Aws.Kinesis
import qualified Data.ByteString.Char8 as B8
import Control.Applicative.Unicode
import Control.Lens
import Control.Monad.Unicode
import Data.Monoid.Unicode
import qualified Data.Text as T
import qualified Data.Text.Lens as T
import Options.Applicative
import Options.Applicative.Types
import Prelude.Unicode

data AccessKeys
  = AccessKeys
  { _akAccessKeyId ∷ !B8.ByteString
  , _akSecretAccessKey ∷ !B8.ByteString
  } deriving Show

data CredentialsInput
  = CredentialsFromAccessKeys AccessKeys
  | CredentialsFromFile FilePath
  deriving Show

data CLIOptions
  = CLIOptions
  { _clioStreamName ∷ !StreamName
  , _clioRegion ∷ !Region
  , _clioLimit ∷ !(Maybe Int)
  , _clioIteratorType ∷ !ShardIteratorType
  , _clioAccessKeys ∷ !(Maybe CredentialsInput)
  , _clioUseInstanceMetadata ∷ !Bool
  , _clioStateIn ∷ !(Maybe FilePath)
  , _clioStateOut ∷ !(Maybe FilePath)
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
    ⊕ help "The path to a file containing your access keys. To be formatted \"default ID SECRET\"; you may provide this instead of the command line options for credentials"

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

stateOutParser ∷ Parser FilePath
stateOutParser =
  strOption $
    long "save-state"
    ⊕ help "Write the last known state of each shard to a file"
    ⊕ metavar "FILE"

stateInParser ∷ Parser FilePath
stateInParser =
  strOption $
    long "restore-state"
    ⊕ help "Read a saved stream state from a file. For any shards whose state is restored, the 'AFTER_SEQUENCE_NUMBER' iterator type will be used; other shards will use the iterator type you have specified. Some shards may have been merged or closed between when the state was saved and restored; at this point, no effort has been made to do anything here beyond the obvious (shards are identified by their shard-id)."
    ⊕ metavar "FILE"

credentialsInputParser ∷ Parser CredentialsInput
credentialsInputParser =
  foldr (<|>) empty $
    [ CredentialsFromAccessKeys <$> accessKeysParser
    , CredentialsFromFile <$> accessKeysPathParser
    ]

useInstanceMetadataParser ∷ Parser Bool
useInstanceMetadataParser =
  switch $
    long "use-instance-metadata"
    ⊕ help "Read the credentials from the instance metadata"

regionReader ∷ ReadM Region
regionReader = do
  fromText ∘ T.pack <$> readerAsk ≫=
    either readerError return


regionParser ∷ Parser Region
regionParser =
  option regionReader $
    long "region"
    ⊕ value UsWest2
    ⊕ help "Choose an AWS Kinesis region (default: us-west-2)"

optionsParser ∷ Parser CLIOptions
optionsParser =
  pure CLIOptions
    ⊛ streamNameParser
    ⊛ regionParser
    ⊛ optional limitParser
    ⊛ iteratorTypeParser
    ⊛ optional credentialsInputParser
    ⊛ useInstanceMetadataParser
    ⊛ optional stateInParser
    ⊛ optional stateOutParser

parserInfo ∷ ParserInfo CLIOptions
parserInfo =
  info (helper ⊛ optionsParser) $
    fullDesc
    ⊕ progDesc "Fetch a given number of records from a Kinesis stream; unlike the standard command line utilities, this interface is suitable for use with a sharded stream. If you both specify a saved stream state to be restored and an iterator type, the latter will be used on any shards which are not contained in the saved state. Minimally, you must specify your AWS credentials, a stream name, and an optional limit."
    ⊕ header "The Kinesis Consumer CLI v0.2.0.1"

