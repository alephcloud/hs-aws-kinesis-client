{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE UnicodeSyntax #-}

-- |
-- Module: CLI.Options
-- Copyright: Copyright © 2014 AlephCloud Systems, Inc.
-- License: MIT
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

