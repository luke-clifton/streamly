{-# LANGUAGE CPP #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

#include "Streams/inline.hs"

-- |
-- Module      : Streamly.FileIO
-- Copyright   : (c) 2018 Harendra Kumar
--
-- License     : BSD3
-- Maintainer  : harendra.kumar@gmail.com
-- Stability   : experimental
-- Portability : GHC
--

module Streamly.FileIO
    ( fromHandle
    , toHandle
    , fromHandleBuffers
    , fromHandleWord8
    , toWord8Stream
    )
where

import Streamly.Buffer
