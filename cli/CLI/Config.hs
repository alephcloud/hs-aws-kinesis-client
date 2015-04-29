-- Copyright (c) 2013-2015 PivotCloud, Inc.
--
-- CLI.Config
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

{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE UnicodeSyntax #-}

-- |
-- Module: CLI.Config
-- Copyright: Copyright © 2013-2015 PivotCloud, Inc.
-- License: Apache-2.0
-- Maintainer: Jon Sterling <jsterling@alephcloud.com>
-- Stability: experimental
--
module CLI.Config
( -- * Configuration Type
  Config(..)
, defaultConfig
  -- ** Lenses
, configStreamName
, configRegion
, configLimit
, configTimeout
, configIteratorType
, configCredentialConfig
, configStateIn
, configStateOut
  -- ** Parser
, pConfig
  -- ** Validation
, validateConfig
) where

import Aws.General
import Aws.Kinesis
import Configuration.Utils
import Configuration.Utils.Aws.Credentials

import Control.Lens hiding ((.=))
import Control.Monad
import Control.Monad.Error.Class
import Control.Monad.Unicode
import Data.Monoid.Unicode
import qualified Data.Text as T
import qualified Data.Text.Lens as T
import Options.Applicative
import Options.Applicative.Types
import Prelude.Unicode

data Config
  = Config
  { _configStreamName ∷ !StreamName
  , _configRegion ∷ !Region
  , _configLimit ∷ !(Maybe Int)
  , _configTimeout ∷ !(Maybe Int)
  , _configIteratorType ∷ !ShardIteratorType
  , _configCredentialConfig ∷ !CredentialConfig
  , _configStateIn ∷ !(Maybe FilePath)
  , _configStateOut ∷ !(Maybe FilePath)
  } deriving Show

invalidStreamName ∷ StreamName
invalidStreamName = "NOT_A_VALID_STREAM_NAME"

defaultConfig ∷ Config
defaultConfig = Config
  { _configStreamName = invalidStreamName
  , _configRegion = UsWest2
  , _configLimit = Nothing
  , _configTimeout = Nothing
  , _configIteratorType = TrimHorizon
  , _configCredentialConfig = defaultCredentialConfig
  , _configStateIn = Nothing
  , _configStateOut = Nothing
  }

makeLenses ''Config

instance FromJSON (Config → Config) where
  parseJSON =
    withObject "Config" $ \o → id
      <$< configStreamName ..: "stream_name" % o
      <*< setProperty configRegion "region" (withText "Region" $ either fail return ∘ fromText) o
      <*< configLimit ..: "limit" % o
      <*< configTimeout ..: "timeout" % o
      <*< configIteratorType ..: "iterator_type" % o
      <*< configCredentialConfig %.: "aws_credentials" % o
      <*< configStateIn ..: "restore_state" % o
      <*< configStateOut ..: "save_state" % o

instance ToJSON Config where
  toJSON Config{..} = object
    [ "stream_name" .= _configStreamName
    , "region" .= String (toText _configRegion)
    , "limit" .= _configLimit
    , "timeout" .= _configTimeout
    , "iterator_type" .= _configIteratorType
    , "aws_credentials" .= _configCredentialConfig
    , "restore_state" .= _configStateIn
    , "save_state" .= _configStateOut
    ]


eitherTextReader
  ∷ (T.IsText i, T.IsText e)
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
    ⊕ help "Fetch `L` records. If a limit is provided, then the run will only be considered successful if it results in the CLI fetching `L` records; otherwise, a run is always considered successful."

timeoutParser ∷ Parser Int
timeoutParser =
  option auto $
    long "timeout"
    ⊕ short 't'
    ⊕ metavar "T"
    ⊕ help "Terminate the consumer after `T` seconds. Even if a limit has been provided, the consumer will terminate after at most `T` seconds."

regionParser ∷ Parser Region
regionParser =
  option regionReader $
    long "region"
    ⊕ value UsWest2
    ⊕ help "Choose an AWS Kinesis region (default: us-west-2)"

regionReader ∷ ReadM Region
regionReader = do
  fromText ∘ T.pack <$> readerAsk ≫=
    either readerError return

iteratorTypeReader ∷ ReadM ShardIteratorType
iteratorTypeReader = do
  readIteratorType ∘ T.pack <$> readerAsk ≫=
    either readerError return

  where
    readIteratorType = \case
      "LATEST" → Right Latest
      "Latest" → Right Latest
      "TRIM_HORIZON" → Right TrimHorizon
      "TrimHorizon" → Right TrimHorizon
      it → Left $ "Unsupported shard iterator type: " ⊕ T.unpack it

iteratorTypeParser ∷ Parser ShardIteratorType
iteratorTypeParser =
  option iteratorTypeReader $
    long "iterator-type"
    ⊕ short 'i'
    ⊕ metavar "IT"
    ⊕ help "Iterator type (LATEST|TRIM_HORIZON)"
    ⊕ value TrimHorizon
    ⊕ showDefault

stateOutParser ∷ Parser FilePath
stateOutParser =
  strOption $
    long "save-state"
    ⊕ help "Write the last known state of each shard to a file; this will only occur when the run has completed in a \"successful\" state."
    ⊕ metavar "FILE"

stateInParser ∷ Parser FilePath
stateInParser =
  strOption $
    long "restore-state"
    ⊕ help "Read a saved stream state from a file. For any shards whose state is restored, the 'AFTER_SEQUENCE_NUMBER' iterator type will be used; other shards will use the iterator type you have specified. Some shards may have been merged or closed between when the state was saved and restored; at this point, no effort has been made to do anything here beyond the obvious (shards are identified by their shard-id)."
    ⊕ metavar "FILE"

pConfig ∷ MParser Config
pConfig = id
  <$< configStreamName .:: streamNameParser
  <*< configRegion .:: regionParser
  <*< configLimit .:: optional limitParser
  <*< configTimeout .:: optional timeoutParser
  <*< configIteratorType .:: iteratorTypeParser
  <*< configCredentialConfig %:: pCredentialConfig "aws-"
  <*< configStateIn .:: optional stateInParser
  <*< configStateOut .:: optional stateOutParser

validateConfig ∷ ConfigValidation Config λ
validateConfig Config{..} = do
  when (_configStreamName == invalidStreamName) $
    throwError "stream name not configured"

  validateCredentialConfig _configCredentialConfig

