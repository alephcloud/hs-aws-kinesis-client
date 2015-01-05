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
-- Copyright: Copyright © 2014 AlephCloud Systems, Inc.
-- License: MIT
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
        =$ CL.mapM_ (liftIO ∘ print ∘ recordData)
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
