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
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE UnicodeSyntax #-}

-- |
-- Module: Main
-- Copyright: Copyright © 2013-2014 PivotCloud, Inc.
-- License: Apache-2.0
-- Maintainer: Jon Sterling <jsterling@alephcloud.com>
-- Stability: experimental
--

module Main where

import Aws.Aws
import Aws.General
import Aws.Kinesis hiding (Record)
import Aws.Kinesis.Client.Common
import Aws.Kinesis.Client.Consumer

import CLI.Options
import CLI.Record

import Control.Applicative
import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Control
import Control.Monad.Trans.Either
import Control.Monad.Trans.Resource
import Control.Monad.Codensity
import Control.Monad.Reader.Class
import Control.Monad.Trans.Reader (ReaderT(..))
import Control.Monad.Error.Class

import Data.Aeson
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as BL
import Data.Conduit
import Data.Monoid
import qualified Data.Conduit.List as CL
import qualified Data.Text.Lens as T

import Options.Applicative
import qualified Network.HTTP.Conduit as HC
import Prelude.Unicode
import Control.Monad.Unicode

type MonadCLI m
  = ( MonadReader CLIOptions m
    , MonadIO m
    , MonadBaseControl IO m
    , MonadError ConsumerError m
    )

identityConduit
  ∷ Monad m
  ⇒ Conduit a m a
identityConduit = CL.map id

limitConduit
  ∷ MonadCLI m
  ⇒ Conduit a m a
limitConduit =
  lift (asks clioLimit) ≫=
    CL.isolate

filterConduit
  ∷ MonadCLI m
  ⇒ Conduit Record m Record
filterConduit = do
  CLIOptions{..} ← lift ask
  CL.filter (\Record{..} → maybe True (rTimestamp ≥) clioStartDate)
    =$ takeTill (\Record{..} → maybe False (rTimestamp >) clioEndDate)

takeTill
  ∷ Monad m
  ⇒ (i → Bool)
  → Conduit i m i
takeTill f = loop
  where
    loop = await ≫= maybe (return ()) (\x → unless (f x) $ yield x ≫ loop)

app
  ∷ MonadCLI m
  ⇒ Codensity m ()
app = do
  CLIOptions{..} ← ask
  manager ← managedHttpManager
  awsConfiguration ← liftIO baseConfiguration
  consumer ← managedKinesisConsumer $ ConsumerKit
    { _ckKinesisKit = KinesisKit
        { _kkManager = manager
        , _kkConfiguration = awsConfiguration
        , _kkKinesisConfiguration = KinesisConfiguration UsWest2
        }
    , _ckStreamName = clioStreamName
    , _ckBatchSize = 100
    , _ckIteratorType = clioIteratorType
    }

  lift $ consumerSource consumer $$
    case clioRaw of
      False →
        CL.mapMaybe (decode ∘ BL.fromChunks ∘ (:[]) ∘ recordData)
          =$ filterConduit
          =$ limitConduit
          =$ CL.mapM_ (liftIO ∘ print)
      True →
        limitConduit
        =$ CL.mapM_ (liftIO ∘ B8.putStrLn ∘ recordData)
  return ()

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
