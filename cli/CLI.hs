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
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnicodeSyntax #-}

-- |
-- Module: Main
-- Copyright: Copyright © 2013-2015 PivotCloud, Inc.
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

import CLI.Config

import Configuration.Utils
import Configuration.Utils.Aws.Credentials

import Control.Concurrent.Lifted
import Control.Concurrent.Async.Lifted
import Control.Exception.Lifted
import Control.Lens
import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Control
import Control.Monad.Trans.Except
import Control.Monad.Codensity
import Control.Monad.Reader.Class
import Control.Monad.Trans.Reader (ReaderT(..))
import Control.Monad.Unicode

import qualified Data.Aeson as A
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy.Char8 as BL8
import Data.Conduit
import qualified Data.Conduit.List as CL
import Data.Monoid.Unicode
import Data.Traversable
import Data.Typeable

import qualified Network.HTTP.Conduit as HC
import Prelude.Unicode
import System.Exit
import System.IO.Error

import PkgInfo_kinesis_cli

data CLIError
  = MissingCredentials
  | NoInstanceMetadataCredentials
  deriving (Typeable, Show)

instance Exception CLIError

type MonadCLI m
  = ( MonadReader Config m
    , MonadIO m
    , MonadBaseControl IO m
    )

managedKinesisKit
  ∷ MonadCLI m
  ⇒ Codensity m KinesisKit
managedKinesisKit = do
  Config{..} ← ask
  manager ← managedHttpManager
  credentials ← lift $ runExceptT (credentialsFromConfig _configCredentialConfig) ≫= either throwIO return
  return KinesisKit
    { _kkManager = manager
    , _kkConfiguration = Configuration
         { timeInfo = Timestamp
         , credentials = credentials
         , logger = defaultLog Warning
         }
    , _kkKinesisConfiguration = defaultKinesisConfiguration _configRegion
    }

cliConsumerKit
  ∷ MonadCLI m
  ⇒ KinesisKit
  → m ConsumerKit
cliConsumerKit kinesisKit = do
  Config{..} ← ask
  savedStreamState ←
    case _configStateIn of
      Just path → liftIO $ do
        code ← catch (Just <$> BL8.readFile path) $ \case
          exn
            | isDoesNotExistError exn → return Nothing
            | otherwise → throwIO exn
        traverse (either (fail ∘ ("Invalid saved state: " ⊕)) return) $
          A.eitherDecode <$> code
      Nothing → return Nothing
  return $
    makeConsumerKit kinesisKit _configStreamName
      & ckBatchSize .~ 100
      & ckIteratorType .~ _configIteratorType
      & ckSavedStreamState .~ savedStreamState

app
  ∷ MonadCLI m
  ⇒ Codensity m ExitCode
app = do
  Config{..} ← ask

  consumer ←
    managedKinesisKit
      ≫= lift ∘ cliConsumerKit
      ≫= managedKinesisConsumer

  let
    source = consumerSource consumer
    step n r = succ n <$ liftIO (B8.putStrLn $ recordData r)
    countingSink = CL.foldM step (1 ∷ Int)
    sink = case _configLimit of
      Just limit → CL.isolate limit =$ countingSink
      Nothing → countingSink

    runConsumer = do
      n ← catch (source $$ sink) $ \SomeException{} → return 0
      return $ maybe True (n ≥) _configLimit


  --  If a timeout is set, then we will race the timeout against the consumer.
  result ← lift $
    case _configTimeout of
      Just seconds → race (threadDelay $ seconds * 1000000) runConsumer
      Nothing → Right <$> runConsumer

  let
    successful =
      case result of
        -- if we timed out: if there a requested limit, then we failed to get
        -- it (and this is a failure); if there was no requested limit, then we
        -- consider this a success.
        Left () → isn't _Just _configLimit

        -- if we did not timeout, then it is a success
        Right b → b

  lift ∘ when successful ∘ void ∘ for _configStateOut $ \outPath → do
    state ← consumerStreamState consumer
    liftIO ∘ BL8.writeFile outPath $ A.encode state

  return $
    if successful
      then ExitSuccess
      else ExitFailure 1

mainInfo ∷ ProgramInfo Config
mainInfo =
  programInfoValidate
    "Kinesis CLI"
    pConfig
    defaultConfig
    validateConfig

main ∷ IO ()
main = do
  runWithPkgInfoConfiguration mainInfo pkgInfo $
    runReaderT (lowerCodensity app)
      >=> exitWith

managedHttpManager
  ∷ ( MonadIO m
    , MonadBaseControl IO m
    )
  ⇒ Codensity m HC.Manager
managedHttpManager =
  Codensity $ HC.withManager ∘ (lift ∘)
