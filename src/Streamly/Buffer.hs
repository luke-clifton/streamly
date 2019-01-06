{-# LANGUAGE CPP #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

#include "Streams/inline.hs"

-- |
-- Module      : Streamly.Buffer
-- Copyright   : (c) 2018 Harendra Kumar
--
-- License     : BSD3
-- Maintainer  : harendra.kumar@gmail.com
-- Stability   : experimental
-- Portability : GHC
--

module Streamly.Buffer
    ( fromHandle
    , toHandle
    , fromHandleBuffers
    , fromHandleWord8
    , toWord8Stream
    , hGetBuffer
    , Buffer (..)
    )
where

import Control.Monad.IO.Class (MonadIO(..))
import Data.Word (Word8)
{-
import GHC.ForeignPtr  (ForeignPtr(ForeignPtr)
                       ,newForeignPtr_, mallocPlainForeignPtrBytes)
-}
import Foreign.C.Types (CSize(..))
import Foreign.ForeignPtr (ForeignPtr, withForeignPtr, touchForeignPtr)
import Foreign.ForeignPtr.Unsafe (unsafeForeignPtrToPtr)
import Foreign.Ptr (Ptr, plusPtr, minusPtr)
import Foreign.Storable (Storable(..))
import GHC.Base (realWorld#)
import GHC.ForeignPtr (mallocPlainForeignPtrBytes)
import GHC.IO (IO(IO))
import System.IO (Handle, hGetBufSome, hPutBuf) -- , hSeek, SeekMode(..))
import System.IO.Unsafe (unsafePerformIO)

import qualified Streamly.Prelude as S

import Streamly.SVar (adaptState)
import Streamly.Streams.Serial (SerialT)
import Streamly.Streams.StreamK.Type (IsStream, mkStream)
import qualified Streamly.Streams.StreamD.Type as D
import qualified Streamly.Streams.StreamD as D

-- XXX this should go in Streamly.String, this will require decoding to be
-- performed. We will need a fromHandleChar as a result of decoding.
-- perhaps a separate streamly-string package that will include normalized
-- strings as well.
--
-- fromHandleLn :: (IsStream t, MonadIO m) => IO.Handle -> t m (t m Char)
-- fromHandleLn h = go

-- XXX this should become fromhandle. or should we reserve fromHandle for
-- general streamed deserialization of a type "a" from file, for example
-- fromHandle h deserializer :: t m a. but we can anyway map the dserializer on
-- the stream like decodeUtf8.
--
-- XXX move it to Streamly.FileIO?

-- XXX we can use address tags in IO buffers to coalesce multiple buffers into
-- fewer IO requests. Similalrly we can split responses to serve them to the
-- right consumers. This will be comonadic.
--
-- XXX we can also have IO error monitors attached to streams. to monitor disk
-- or network errors or latencies and then take actions for example starting a
-- disk scrub or switching to a different location on the network.
--
-------------------------------------------------------------------------------
-- Buffers
-------------------------------------------------------------------------------

-- Buffers are chunks of memory that can hold arbitrary values, but usually
-- used to hold self-contained data structures fully evaluated and serialized
-- to Word8 arrays from Haskell values. Buffer use memory that is out of the
-- ambit of GC and therefore add no pressure to GC other than the Buffer
-- pointer itself.
--
-- Buffers help when we want to hold large amounts of data. Too many small
-- buffers (e.g. single byte) are only as good as holding data in a Haskell
-- list. However, small buffers can be compacted into large ones to reduce the
-- overhead. To hold 32GB memory in 32k sized buffers we will need 1 million
-- buffers.  This is still significant to add pressure to GC. If we want to
-- hold such large amounts of data outside GC then we can implement chained
-- buffers i.e.  a linked list of buffers in non-GC memory.
--
-- We can also use a "Compact a" type for a compact region buffer
-- Do we need an alignment argument?
-- XXX add reverse flag to reverse the bytes
-- if we use "Buffer a/Vector a" where a is of fixed size i.e. it does not have
-- a recursive structure (and if we can determine and enforce that at compile
-- time) then we can reverse any vector of a without doing anything.
-- We can use a vector like typeclass to abstract the operations and only fixed
-- size types are made instances of it.
-- When converting a stream to a buffer we statically know the size of an
-- element and we can determine how many elements we need to collect in one
-- chunk so that we can allocate in fixed size chunks rather than growing one
-- element at a time.
data Buffer = Buffer {-# UNPACK #-} !(ForeignPtr Word8)
                     {-# UNPACK #-} !Int

sizeOfBuf :: Buffer -> Int
sizeOfBuf (Buffer _ s) = s

foreign import ccall unsafe "string.h memcpy" c_memcpy
    :: Ptr Word8 -> Ptr Word8 -> CSize -> IO (Ptr Word8)

memcpy :: Ptr Word8 -> Ptr Word8 -> Int -> IO ()
memcpy p q s = c_memcpy p q (fromIntegral s) >> return ()

resizeBuffer :: Ptr Word8 -> Int -> Int -> IO Buffer
resizeBuffer pOld oldSize newSize = do
    newPtr <- mallocPlainForeignPtrBytes newSize
    withForeignPtr newPtr $ \pNew -> do
        memcpy pNew pOld (min newSize oldSize)
        return $! Buffer newPtr newSize

-- | GHC memory management allocation header overhead
allocOverhead :: Int
allocOverhead = 2 * sizeOf (undefined :: Int)

-- | Default buffer size in bytes. Account for the GHC memory allocation
-- overhead so that the actual allocation is rounded to page boundary.
defaultReadBufSize :: Int
defaultReadBufSize = 320 * k - allocOverhead
   where k = 1024

-- XXX we can use newtype over stream for buffers. That way we can implement
-- operations like length as a fold of length of all underlying buffers.
-- A single buffer could be a singleton stream and more than one buffers would
-- be a stream of buffers.
--
-- Also, if a single buffer size is more than a threshold we can store it as a
-- linked list in the non-gc memory. This will allow unlimited size buffers to
-- be stored.
--
-- A vector is a special type of buffer which serializes only the top level
-- array and not the elements. So a "Buffer a" would in fact be a vector and
-- "Buffer" would be a "Buffer Word8". So a vector becomes a special case of a
-- single large Buffer.
--
-- In the vector implementation, we can start with building it as a stream and
-- compact it after a threshold is reached. That way we do not have to
-- reallocate memory too often and we also do not have to allocate a chunk
-- upfront. It would be a stream of buffers and a buffer's representation could
-- be either as a stream or as an array. We will have to store the count of
-- elements even in a streamed representation. This could work well in general
-- without having to know whether we are storing or not. If the stream becomes
-- larger than one segment then we would compress/serialize the pervious
-- segement before adding a new segment. If the number of these buffers becomes
-- too large then we can again compress many of them into a larger bucket. That
-- way we can build a tree incrementally. And aggregation information like
-- space accounting (length of the buffer/index) can be kept at each node. That
-- way we can address any element by its index quickly even if it is not really
-- a completely flat array. We can also insert randomly if we make this a sort
-- of B+ tree.

-------------------------------------------------------------------------------
-- Converting buffers to and from streams of Word8
-------------------------------------------------------------------------------

{-# INLINE accursedUnutterablePerformIO #-}
accursedUnutterablePerformIO :: IO a -> a
accursedUnutterablePerformIO (IO m) = case m realWorld# of (# _, r #) -> r

-- toStreamWord16/32/64
-- fromStreamWord16/32/64
-- use concatMap toStreamWord8 to convert a stream of buffers to stream of
-- Word8
{-# INLINE toWord8StreamD #-}
toWord8StreamD :: Monad m => Buffer -> D.Stream m Word8
toWord8StreamD (Buffer fptr len) =
    -- XXX use foldr for buffer
    let p = unsafeForeignPtrToPtr fptr
    in D.Stream step (p, p `plusPtr` len)

    where

    {-# INLINE_LATE step #-}
    step _ (p, end) | p == end = return D.Stop
    step _ (p, end) =
        let !x = peekFptr fptr p
        in return $ D.Yield x (p `plusPtr` 1, end)

    {-# INLINE peekFptr #-}
    peekFptr fp p = accursedUnutterablePerformIO $ do
        x <- peek p
        touchForeignPtr fp
        return x

{-# INLINE toWord8Stream #-}
toWord8Stream :: (IsStream t, Monad m) => Buffer -> t m Word8
toWord8Stream buf = D.fromStreamD $ toWord8StreamD buf

data Word8ToBufferState =
      BufAlloc
    | BufWrite (ForeignPtr Word8) (Ptr Word8) (Ptr Word8)
    | BufStop

-- XXX we should never have zero sized chunks if we want to use "null" on a
-- stream of buffers to mean that the stream itself is null.
{-# INLINE fromWord8StreamD #-}
fromWord8StreamD :: Monad m => Int -> D.Stream m Word8 -> D.Stream m Buffer
fromWord8StreamD bufSize (D.Stream step state) =
    D.Stream step' (state, BufAlloc)

    where

    {-# INLINE_LATE step' #-}
    step' _ (st, BufAlloc) =
        let !res = unsafePerformIO $ do
                fptr <- mallocPlainForeignPtrBytes bufSize
                let p = unsafeForeignPtrToPtr fptr
                return $ D.Skip $ (st, BufWrite fptr p (p `plusPtr` bufSize))
        in return res

    step' _ (st, BufWrite fptr cur end) | cur == end =
        return $ D.Yield (Buffer fptr bufSize) (st, BufAlloc)

    step' gst (st, BufWrite fptr cur end) = do
        res <- step (adaptState gst) st
        return $ case res of
            D.Yield x s ->
                let !r = accursedUnutterablePerformIO $ do
                            poke cur x
                            -- XXX do we need a touch here?
                            return $ D.Skip (s, BufWrite fptr (cur `plusPtr` 1) end)
                in r
            D.Skip s -> D.Skip (s, BufWrite fptr cur end)
            D.Stop ->
                -- XXX resize the buffer
                D.Yield (Buffer fptr (bufSize + (cur `minusPtr` end)))
                        (st, BufStop)

    step' _ (_, BufStop) = return D.Stop

{-# INLINE fromWord8Stream #-}
fromWord8Stream :: (IsStream t, Monad m) => t m Word8 -> t m Buffer
fromWord8Stream str =
    D.fromStreamD $ fromWord8StreamD defaultReadBufSize (D.toStreamD str)

-------------------------------------------------------------------------------
-- Reading a stream from a file handle
-------------------------------------------------------------------------------

-- XXX need to test/design the API for different devices (block devices, char
-- devices, fifos, sockets).

{-# INLINE hGetBuffer #-}
hGetBuffer :: Handle -> Int -> IO Buffer
hGetBuffer h size = do
    ptr <- mallocPlainForeignPtrBytes size
    withForeignPtr ptr $ \p -> do
        n <- hGetBufSome h p size
        case compare n size of
            EQ -> return $! Buffer ptr size
            -- XXX resize only if the diff is significant
            LT -> resizeBuffer p size n
            GT -> error "Panic: hGetBufSome read more than the size of buf"

{-# INLINE fromHandleBuffers #-}
fromHandleBuffers :: (IsStream t, MonadIO m) => Handle -> Int -> t m Buffer
fromHandleBuffers handle bufSize = go
  where
    -- XXX use cons/nil instead
    go = mkStream $ \_ yld sng _ -> do
        buf <- liftIO $ hGetBuffer handle bufSize
        if sizeOfBuf buf < bufSize
        then sng buf
        else yld buf go

{-
-- XXX handles could be shared, so we do not want to use the handle state at
-- all for this API. we should use pread and pwrite instead. On windows we will
-- need to use readFile/writeFile with an offset argument. Note, we can do this
-- only on seekable handles.
--
-- @fromHandleChunksAt handle size at@ generates a stream of 'Chunks' from the
-- file handle @handle@, the maximum size of chunks is @size@ and the stream
-- starts at offset @at@ in the file. The stream ends when the file ends. The
-- resulting chunks may be shorter than the specified size but never more than
-- it.
fromHandleBuffersAt
    :: (IsStream t, MonadIO m, MonadIO (t m))
    => Handle -> Int -> Int -> t m Buffer
fromHandleBuffersAt handle bufSize at = do
    liftIO $ hSeek handle AbsoluteSeek (fromIntegral at)
    fromHandleBuffers handle bufSize

-- @fromHandleSizedAt handle granularity at@ generate a stream of 'Word8' from
-- the file handle @handle@, performing IO in chunks of size @granularity@ and
-- starting at the offset @at@. The stream ends when the file ends.
fromHandleWord8At
    :: (IsStream t, MonadIO m, MonadIO (t m))
    => Handle -> Int -> Int -> t m Word8
fromHandleWord8At h chunkSize offset =
    S.concatMap toWord8Stream $ fromHandleBuffersAt h chunkSize offset
-}

{-# INLINE fromHandleWord8 #-}
fromHandleWord8 :: (IsStream t, MonadIO m) => Handle -> Int -> t m Word8
fromHandleWord8 h chunkSize =
    S.concatMap toWord8Stream $ fromHandleBuffers h chunkSize

-- XXX for concurrent streams implement readahead IO. We can send multiple read
-- requests at the same time. For serial case we can use async IO. We can also
-- control the read throughput in mbps or IOPS.
{-# INLINE fromHandle #-}
fromHandle :: (IsStream t, MonadIO m) => Handle -> t m Word8
fromHandle h = fromHandleWord8 h defaultReadBufSize

-------------------------------------------------------------------------------
-- Writing a stream to a file handle
-------------------------------------------------------------------------------

hPutBuffer :: Handle -> Buffer -> IO ()
hPutBuffer _ (Buffer _  0) = return ()
hPutBuffer h (Buffer fptr len) = withForeignPtr fptr $ \p -> hPutBuf h p len

toHandle :: MonadIO m => Handle -> SerialT m Word8 -> m ()
toHandle h m = S.mapM_ (liftIO . hPutBuffer h) $ fromWord8Stream m

{-
-- We do not provide ways to combine or operate on buffers directly as it will
-- involve a copy. The only way to combine buffers is via streams, we can
-- create a stream of buffers, concat it and serialize it again to a new
-- buffer. This way it is explicit that we are recreating a buffer.  When
-- serializing a stream of buffers to a single buffer we can use memcpy to copy
-- each buffer into the destination buffer.

-- we can just use a fold using Monoid
-- concatBuffers :: t m Buffer -> m Buffer
splitBuffer :: Int -> Buffer -> t m Buffer
splitBuffer maxSize buf =

-- compactBuffers :: Int -> Int -> t m Buffer -> t m Buffer
-- compactBuffers minSize tolerance =
--
-- deCompactBuffers :: Int -> Int -> t m Buffer -> t m Buffer
-- deCompactBuffers maxSize tolerance =

-- When each IO opration has a significant system overhead, it may be more
-- efficient to do gather IO. But when the buffers are too small we may want to
-- copy multiple of them in a single buffer rather than setting up a gather
-- list. A gather list may have more overhead compared to just copying. If the
-- buffer is larger than a limit we may just keep a single buffer in a gather
-- list.
--
-- gatherBuffers :: Int -> t m Buffer -> t m GatherBuffer
-- gatherBuffers maxLimit bufs =
-}