{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE UnboxedTuples #-}
{-# LANGUAGE ScopedTypeVariables #-}

#include "Streams/inline.hs"

-- |
-- Module      : Streamly.Vector
-- Copyright   : (c) 2018 Harendra Kumar
--
-- License     : BSD3
-- Maintainer  : harendra.kumar@gmail.com
-- Stability   : experimental
-- Portability : GHC
--
-- Vectors are chunks of memory that can hold a /finite/ sequence of values of
-- the same type. Unlike streams vectors are /finite/ and therefore most of the
-- APIs dealing with vectors specify the size of the vector. The size of a
-- vector is pre-determined unlike streams where we need to compute the length
-- by traversing the entire stream.
--
-- Most importantly, vectors as implemented in this module, use memory that is
-- out of the ambit of GC and therefore add no pressure to GC. Moreover, they
-- can be used to communicate with foreign consumers and producers (e.g. file
-- and network IO) with zero copy.
--
-- Vectors help reduce GC pressure when we want to hold large amounts of data
-- in memory. Too many small vectors (e.g. single byte) are only as good as
-- holding data in a Haskell list. However, small vectors can be compacted into
-- large ones to reduce the overhead. To hold 32GB memory in 32k sized buffers
-- we need 1 million vectors. This is still significant to add pressure to GC.
-- However, we can create vectors of vectors (trees) to scale to arbitrarily
-- large amounts of memory but still using small chunks of contiguous memory.

-------------------------------------------------------------------------------
-- Design Notes
-------------------------------------------------------------------------------

-- There are two goals that we need to fulfill and use vectors to fulfill them.
-- One, holding large amounts of data in non-GC memory, two, allow random
-- access to elements based on index. The first ones fall in the category of
-- storage buffers while the second ones fall in the category of
-- maps/multisets/hashmaps.
--
-- For the first requirement we use a vector of Storables. We can have both
-- immutable and mutable variants of this vector using wrappers over the same
-- underlying type.
--
-- For the second requirement we can provide a vector of polymorphic elements
-- that need not be Storable instances. In that case we need to use an Array#
-- instead of a ForeignPtr. This type of vector would not reduce the GC
-- overhead as much because each element of the array still needs to be scanned
-- by the GC.  However, this would allow random access to the elements. But in
-- most cases random access means storage, and it means we need to avoid GC
-- scanning except in cases of trivially small storage. One way to achieve that
-- would be to put the array in a Compact region. However, when we mutate this
-- we will have to use a manual GC copying out to another CR and freeing the
-- old one.

-------------------------------------------------------------------------------
-- SIMD Vectors
-------------------------------------------------------------------------------

-- XXX Try using SIMD operations where possible to combine vectors and to fold
-- vectors. For example computing checksums of streams, adding streams or
-- summing streams.

-------------------------------------------------------------------------------
-- Caching coalescing/batching
-------------------------------------------------------------------------------

-- XXX we can use address tags in IO buffers to coalesce multiple buffers into
-- fewer IO requests. Similarly we can split responses to serve them to the
-- right consumers. This will be comonadic. A common buffer cache can be
-- maintained which can be shared by many consumers.
--
-- XXX we can also have IO error monitors attached to streams. to monitor disk
-- or network errors or latencies and then take actions for example starting a
-- disk scrub or switching to a different location on the network.

-------------------------------------------------------------------------------
-- Representation notes
-------------------------------------------------------------------------------

-- XXX we can use newtype over stream for buffers. That way we can implement
-- operations like length as a fold of length of all underlying buffers.
-- A single buffer could be a singleton stream and more than one buffers would
-- be a stream of buffers.
--
-- Also, if a single buffer size is more than a threshold we can store it as a
-- linked list in the non-gc memory. This will allow unlimited size buffers to
-- be stored.
--
-- Unified Vector + Stream:
-- We can use a "Vector" as the unified stream structure. When we use a cons we
-- can increase the count of buffered elements in the stream, when we use
-- uncons we decrement the count. If the count goes beyond a threshold we
-- vectorize the buffered part. So if we are accessing an index at the end of
-- the stream and still want to hold on to the stream elements then we would be
-- buffering it by consing the elements and therefore automatically vectorizing
-- it. By consing we are joining an evaluated part with a potentially
-- unevaluated tail therefore strictizing the stream. When we uncons we are
-- actually taking out a vector (i.e. evaluated part, WHNF of course) from the
-- stream.

module Streamly.Vector
    (
      Vector (..)
    , resizePtr
    , length

    -- * Construction/Generation
    , readHandleWith
    , bufferN

    -- * Elimination/Folds
    , writeHandle
    , vConcat
    )
where

import Control.Monad.IO.Class (MonadIO(..))
import Data.Int (Int64)
import Data.Word (Word8)
import Foreign.C.Types (CSize(..))
import Foreign.ForeignPtr
       (ForeignPtr, withForeignPtr, touchForeignPtr)
import Foreign.ForeignPtr.Unsafe (unsafeForeignPtrToPtr)
import Foreign.Ptr (Ptr, plusPtr, minusPtr, castPtr)
import Foreign.Storable (Storable(..))
import GHC.Base (realWorld#)
import GHC.ForeignPtr (mallocPlainForeignPtrBytes)
import GHC.IO (IO(IO))
import System.IO (Handle, hGetBufSome, hPutBuf)
import System.IO.Unsafe (unsafePerformIO)
import Prelude hiding (length)

import Streamly.SVar (adaptState)
import Streamly.Streams.Serial (SerialT)
import Streamly.Streams.StreamK.Type (IsStream, mkStream)

import qualified Streamly.Prelude as S
import qualified Streamly.Streams.StreamD.Type as D
import qualified Streamly.Streams.StreamD as D

-------------------------------------------------------------------------------
-- Utility functions
-------------------------------------------------------------------------------

foreign import ccall unsafe "string.h memcpy" c_memcpy
    :: Ptr Word8 -> Ptr Word8 -> CSize -> IO (Ptr Word8)

-- XXX we are converting Int to CSize
memcpy :: Ptr Word8 -> Ptr Word8 -> Int -> IO ()
memcpy p q s = c_memcpy p q (fromIntegral s) >> return ()

-------------------------------------------------------------------------------
-- Vector Data Type
-------------------------------------------------------------------------------

-- Storable a
-- data Vector a =
--    VBlock Int (ForeignPtr a)
--  | VTree  Int Int (Vector (Vector a)) -- VTree Size TreeLevel Tree
--
-- The VBlock constructor can be thought of as a stream of single vector.
-- VTree can make the structure hierarchical, we can have vectors inside
-- vectors up to many levels making it a tree. The level of the tree depends on
-- the block size of the vector. We can reduce the level by increasing the
-- block size.
--
-- The block size of a chunk can either be constant or variable. Constant block
-- size would require compacting, whereas variable block size would require
-- more work when searching/accessing an element. To reduce the access overhead
-- we can use a B+ tree for variable sized blocks.
--
-- Use rewrite rules to rewrite vector from and to stream ops to id.

-- XXX Do we need some alignment for the allocations?
-- XXX add reverse flag to reverse the contents without actually reversing.
data Vector a = Vector
    { vPtr  :: {-# UNPACK #-} !(ForeignPtr a)
    , vLen  :: {-# UNPACK #-} !Int   -- number of items of type a
    , vSize :: {-# UNPACK #-} !Int   -- allocated buffer size in bytes
                                     -- XXX in terms of count of 'a' instead?
    }

type ByteVector = Vector Word8

length :: Vector a -> Int
length = vLen

-- sizeOf :: Vector a -> Int
-- sizeOf = vSize

resizePtr :: Int -> Ptr a -> Int -> Int -> IO (Vector a)
resizePtr len pOld oldSize newSize = do
    newPtr <- mallocPlainForeignPtrBytes newSize
    withForeignPtr newPtr $ \pNew -> do
        memcpy (castPtr pNew) (castPtr pOld) (min newSize oldSize)
        return $! Vector {vPtr = newPtr, vLen = len, vSize = newSize}

-- XXX can we remove the IO monad requirement
shrink :: forall a. Storable a => Vector a -> IO (Vector a)
shrink Vector{..} = do
    let newSize = vLen * sizeOf (undefined :: a)
    newPtr <- mallocPlainForeignPtrBytes newSize
    withForeignPtr vPtr $ \pOld -> do
        withForeignPtr newPtr $ \pNew -> do
            memcpy (castPtr pNew) (castPtr pOld) newSize
            return $! Vector {vPtr = newPtr, vLen = vLen, vSize = newSize}

-------------------------------------------------------------------------------
-- Construction/generation of a stream of vectors
-------------------------------------------------------------------------------

-- | Read a 'ByteVector' from a file handle. If no data is available on the
-- handle it blocks until some data becomes available. If data is available
-- then it immediately returns that data without blocking. It reads a maximum
-- of up to the size requested.
{-# INLINE fromHandleSome #-}
fromHandleSome :: Int -> Handle -> IO ByteVector
fromHandleSome size h = do
    ptr <- mallocPlainForeignPtrBytes size
    withForeignPtr ptr $ \p -> do
        n <- hGetBufSome h p size
        case compare n size of
            EQ -> return $! Vector {vPtr = ptr, vLen = n, vSize = size}
            -- XXX resizePtr only if the diff is significant
            LT -> resizePtr n p size n
            GT -> error "Panic: hGetBufSome read more than the size of buf"

-- | @readHandleWith size h@ reads a stream of vectors from file handle @h@.
-- The maximum size of a single vector is limited to @size@.
{-# INLINE readHandleWith #-}
readHandleWith :: (IsStream t, MonadIO m) => Int -> Handle -> t m ByteVector
readHandleWith size h = go
  where
    -- XXX use cons/nil instead
    go = mkStream $ \_ yld sng _ -> do
        vec <- liftIO $ fromHandleSome size h
        if length vec < size
        then sng vec
        else yld vec go

-------------------------------------------------------------------------------
-- Buffer streams into vectors
-------------------------------------------------------------------------------

{-# INLINE accursedUnutterablePerformIO #-}
accursedUnutterablePerformIO :: IO a -> a
accursedUnutterablePerformIO (IO m) = case m realWorld# of (# _, r #) -> r

data ToVectorState a =
      BufAlloc
    | BufWrite (ForeignPtr a) (Ptr a) (Ptr a)
    | BufStop

-- XXX we should never have zero sized chunks if we want to use "null" on a
-- stream of buffers to mean that the stream itself is null.
--
-- n is the number of items in each generated vector.
{-# INLINE toVectorStreamD #-}
toVectorStreamD
    :: forall m a.
       (Monad m, Storable a)
    => Int -> D.Stream m a -> D.Stream m (Vector a)
toVectorStreamD n (D.Stream step state) =
    D.Stream step' (state, BufAlloc)

    where

    size = n * sizeOf (undefined :: a)

    {-# INLINE_LATE step' #-}
    step' _ (st, BufAlloc) =
        let !res = unsafePerformIO $ do
                fptr <- mallocPlainForeignPtrBytes size
                let p = unsafeForeignPtrToPtr fptr
                return $ D.Skip $ (st, BufWrite fptr p (p `plusPtr` n))
        in return res

    step' _ (st, BufWrite fptr cur end) | cur == end =
        return $ D.Yield (Vector {vPtr = fptr, vLen = n, vSize = size})
                         (st, BufAlloc)

    step' gst (st, BufWrite fptr cur end) = do
        res <- step (adaptState gst) st
        return $ case res of
            D.Yield x s ->
                let !r = accursedUnutterablePerformIO $ do
                            poke cur x
                            -- XXX do we need a touch here?
                            return $ D.Skip
                                (s, BufWrite fptr (cur `plusPtr` 1) end)
                in r
            D.Skip s -> D.Skip (s, BufWrite fptr cur end)
            D.Stop ->
                -- XXX resizePtr the buffer
                D.Yield (Vector { vPtr = fptr
                                , vLen = n + (cur `minusPtr` end)
                                , vSize = size})
                        (st, BufStop)

    step' _ (_, BufStop) = return D.Stop

--  When converting a whole stream to a single vector, we can keep adding new
--  levels to a vector tree, creating vectors of vectors so that we do not have
--  to keep reallocating and copying the old data to new buffers. We can later
--  reduce the levels by compacting the tree if want to. The 'hi' argument is
--  to raise an exception if the total size exceeds this limit, this is a
--  safety catch so that we do not vectorize infinite streams and then run out
--  of memory.
--
{-
{-# INLINE toVector #-}
toVector :: (IsStream t, Monad m) => Int -> t m a -> Vector a
-- toVector chunkSize hi stream =
toVector stream =
-}

-------------------------------------------------------------------------------
-- Buffering primitives that do not look at the values
-------------------------------------------------------------------------------

-- These APIs can also be named as collectXXX or accumXXX or vectorizeXXX or
-- groupXXX or batchXXX or vectorXXX
--
{-
-- Wait until min elements are collected irrespective of time. After collecting
-- minimum elements if timeout occurs return the buffer immediately else wait
-- upto timeout or max limit.
bufferTimedMinUpTo :: Int -> Int -> Int -> t m a -> t m (Vector a)
bufferTimedMinUpTo lo hi time =

-- This can be useful if the input stream may "suspend" before generating
-- further output. So we can emit a vector early without waiting.
bufferUpTo :: Int -> t m a -> t m (Vector a)
bufferUpTo hi stream =

-- wait for minimum amount to be collected but don't wait for the upper limit
-- if the input stream suspends.
bufferMinUpTo :: Int -> Int -> t m a -> t m (Vector a)
bufferMinUpTo lo hi stream =

-- Buffer upto a max count or until timeout occurs. If timeout occurs without a
-- single element in the buffer it raises an exception.
--
bufferTimedUpTo :: Int -> Int -> t m a -> t m (Vector a)
bufferTimedUpTo hi time =
-}

-- XXX toVectorStream
-- | @bufferN n stream@ buffers every N elements of a stream into a Vector and
-- return a stream of 'Vector's.
{-# INLINE bufferN #-}
-- bufferN :: Int -> t m a -> t m (Vector a)
bufferN :: (IsStream t, Monad m) => Int -> t m Word8 -> t m ByteVector
bufferN n str =
    D.fromStreamD $ toVectorStreamD n (D.toStreamD str)

{-
-------------------------------------------------------------------------------
-- Buffering primitives that look at the values
-------------------------------------------------------------------------------

-- XXX There could be a slew of APIs similar to the non-value aware ones that
-- look at timestamped values in the stream and collect items based on that.

-- The buffer is emitted as soon as a complete marker sequence is detected if
-- the max limit hits before a marker is detected then an exception is raised.
-- The emitted buffer contains the marker sequence as suffix.
--
bufferMarkerUpTo :: Vector a -> t m a -> t m (Vector a)
bufferMarkerUpTo hi marker stream =

-- Buffer until the next element in sequence arrives. The function argument
-- determines the difference in sequence numebrs. For example, TCP reassembly
-- buffer.
bufferReorder :: (a -> a -> Int) -> t m a -> t m (Vector a)
-}

-------------------------------------------------------------------------------
-- Compact buffers
-------------------------------------------------------------------------------

{-
-- we can call these regroupXXX or reVectorXXX
--
-- Compact buffers in a stream such that each resulting buffer contains exactly
-- N elements.
compactN :: Int -> Int -> t m (Vector a) -> t m (Vector a)
compactN n vectors =

-- This can be useful if the input stream may "suspend" before generating
-- further output. So we can emit a vector early without waiting. It will emit
-- a vector of at least 1 element.
compactUpTo :: Int -> t m (Vector a) -> t m (Vector a)
compactUpTo hi vectors =

-- wait for minimum amount to be collected but don't wait for the upper limit
-- if the input stream suspends. But never go beyond the upper limit.
compactMinUpTo :: Int -> Int -> t m (Vector a) -> t m (Vector a)
compactMinUpTo lo hi vectors =

-- The buffer is emitted as soon as a complete marker sequence is detected. The
-- emitted buffer contains the sequence as suffix.
compactUpToMarker :: Vector a -> t m (Vector a) -> t m (Vector a)
compactUpToMarker hi marker =

-- Buffer upto a max count or until timeout occurs. If timeout occurs without a
-- single element in the buffer it raises an exception.
compactUpToWithTimeout :: Int -> Int -> t m (Vector a) -> t m (Vector a)
compactUpToWithTimeout hi time =

-- Wait until min elements are collected irrespective of time. After collecting
-- minimum elements if timeout occurs return the buffer immediately else wait
-- upto timeout or max limit.
compactInRangeWithTimeout ::
    Int -> Int -> Int -> t m (Vector a) -> t m (Vector a)
compactInRangeWithTimeout lo hi time =

-- Compact the contiguous sequences into a single vector.
compactToReorder :: (a -> a -> Int) -> t m (Vector a) -> t m (Vector a)

-------------------------------------------------------------------------------
-- deCompact buffers
-------------------------------------------------------------------------------

-- split buffers into smaller buffers
-- deCompactBuffers :: Int -> Int -> t m Buffer -> t m Buffer
-- deCompactBuffers maxSize tolerance =

-------------------------------------------------------------------------------
-- Scatter/Gather IO
-------------------------------------------------------------------------------

-- When each IO opration has a significant system overhead, it may be more
-- efficient to do gather IO. But when the buffers are too small we may want to
-- copy multiple of them in a single buffer rather than setting up a gather
-- list. In that case, a gather list may have more overhead compared to just
-- copying. If the buffer is larger than a limit we may just keep a single
-- buffer in a gather list.
--
-- gatherBuffers :: Int -> t m Buffer -> t m GatherBuffer
-- gatherBuffers maxLimit bufs =
-}

-------------------------------------------------------------------------------
-- Nesting/Layers
-------------------------------------------------------------------------------

-- A stream of vectors can be grouped to create vectors of vectors i.e. a tree
-- of vectors. A tree of vectors can be concated to reduce the level of the
-- tree or turn it into a single vector.

-------------------------------------------------------------------------------
-- Elimination/folding
-------------------------------------------------------------------------------

-- XXX shall we have a ByteVector module for Word8 routines?

-- | Writing a stream to a file handle
{-# INLINE toHandle #-}
toHandle :: Handle -> ByteVector -> IO ()
toHandle _ Vector{..} | vLen == 0 = return ()
toHandle h Vector{..} = withForeignPtr vPtr $ \p -> hPutBuf h p vLen

-- XXX we should use overWrite/write
{-# INLINE writeHandle #-}
writeHandle :: MonadIO m => Handle -> SerialT m ByteVector -> m ()
writeHandle h m = S.mapM_ (liftIO . toHandle h) m

{-# INLINE fromVectorD #-}
fromVectorD :: (Monad m, Storable a) => Vector a -> D.Stream m a
fromVectorD Vector{..} =
    -- XXX use foldr for buffer
    let p = unsafeForeignPtrToPtr vPtr
    in D.Stream step (p, p `plusPtr` vLen)

    where

    {-# INLINE_LATE step #-}
    step _ (p, end) | p == end = return D.Stop
    step _ (p, end) =
        let !x = peekFptr vPtr p
        in return $ D.Yield x (p `plusPtr` 1, end)

    {-# INLINE peekFptr #-}
    peekFptr fp p = accursedUnutterablePerformIO $ do
        x <- peek p
        touchForeignPtr fp
        return x

{-# INLINE fromVector #-}
fromVector :: (IsStream t, Monad m, Storable a) => Vector a -> t m a
fromVector = D.fromStreamD . fromVectorD

-- XXX this should perhas go in the Prelude or another module.
-- | Convert a stream of Word8 Vectors into a stream of Word8
{-# INLINE vConcat #-}
vConcat :: (IsStream t, Monad m, Storable a) => t m (Vector a) -> t m a
vConcat = S.concatMap fromVector
