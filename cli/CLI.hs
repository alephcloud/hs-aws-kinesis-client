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

{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnicodeSyntax #-}

-- |
-- Module: Main
-- Copyright: Copyright © 2013-2014 PivotCloud, Inc.
-- License: Apache-2.0
-- Maintainer: Jon Sterling <jsterling@alephcloud.com>
-- Stability: experimental
--
module Main
( main
) where

import Aws
import Aws.Kinesis hiding (Record)
import Aws.Kinesis.Client.Common
import Aws.Kinesis.Client.Consumer

import CLI.Options

import Control.Exception.Lifted
import Control.Lens
import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Control
import Control.Monad.Trans.Either
import Control.Monad.Codensity
import Control.Monad.Reader.Class
import Control.Monad.Trans.Reader (ReaderT(..))
import Control.Monad.Error.Class
import Control.Monad.Error.Hoist
import Control.Monad.Unicode

import qualified Data.Aeson as A
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy.Char8 as BL8
import Data.Conduit
import qualified Data.Conduit.List as CL
import Data.Monoid.Unicode
import Data.Traversable
import Data.Typeable

import Options.Applicative
import qualified Network.HTTP.Conduit as HC
import Prelude.Unicode
import System.IO.Error

data CLIError
  = MissingCredentials
  | NoInstanceMetadataCredentials
  deriving (Typeable, Show)

instance Exception CLIError

type MonadCLI m
  = ( MonadReader CLIOptions m
    , MonadIO m
    , MonadBaseControl IO m
    , MonadError ConsumerError m
    )

fetchCredentials
  ∷ MonadCLI m
  ⇒ m Credentials
fetchCredentials = do
  view clioUseInstanceMetadata ≫= \case
    True →
      loadCredentialsFromInstanceMetadata
        <!?> KinesisError (toException NoInstanceMetadataCredentials)
    False →
      view clioAccessKeys ≫= \case
        Just (CredentialsFromAccessKeys aks) →
          makeCredentials
            (aks ^. akAccessKeyId)
            (aks ^. akSecretAccessKey)
        Just (CredentialsFromFile path) →
          loadCredentialsFromFile path credentialsDefaultKey
            <!?> KinesisError (toException MissingCredentials)
        Nothing →
          throwError $ KinesisError (toException MissingCredentials)

app
  ∷ MonadCLI m
  ⇒ Codensity m ()
app = do
  CLIOptions{..} ← ask
  manager ← managedHttpManager
  credentials ← lift fetchCredentials
  savedStreamState ←
    case _clioStateIn of
      Just path → liftIO $ do
        code ← catch (Just <$> BL8.readFile path) $ \case
          exn
            | isDoesNotExistError exn → return Nothing
            | otherwise → throw exn
        traverse (either (fail ∘ ("Invalid saved state: " ⊕)) return) $
          A.eitherDecode <$> code
      Nothing → return Nothing

  consumer ← managedKinesisConsumer ConsumerKit
    { _ckKinesisKit = KinesisKit
        { _kkManager = manager
        , _kkConfiguration = Configuration
             { timeInfo = Timestamp
             , credentials = credentials
             , logger = defaultLog Warning
             }
        , _kkKinesisConfiguration = KinesisConfiguration _clioRegion
        }
    , _ckStreamName = _clioStreamName
    , _ckBatchSize = 100
    , _ckIteratorType = _clioIteratorType
    , _ckSavedStreamState = savedStreamState
    }

  let
    source = consumerSource consumer
    presink = CL.mapM_ $ liftIO ∘ B8.putStrLn ∘ recordData
    sink = case _clioLimit of
      Just limit → CL.isolate limit =$ presink
      Nothing → presink

  lift ∘ finally (source $$ sink) $
    void ∘ for _clioStateOut $ \outPath → do
      state ← consumerStreamState consumer
      liftIO ∘ BL8.writeFile outPath $ A.encode state

main ∷ IO ()
main =
  eitherT (fail ∘ show) return $
    liftIO (execParser parserInfo)
      ≫= runReaderT (lowerCodensity app)

managedHttpManager
  ∷ ( MonadIO m
    , MonadBaseControl IO m
    )
  ⇒ Codensity m HC.Manager
managedHttpManager =
  Codensity $ HC.withManager ∘ (lift ∘)
