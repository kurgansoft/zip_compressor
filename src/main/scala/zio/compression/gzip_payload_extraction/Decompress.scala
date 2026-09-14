package zio.compression.gzip_payload_extraction

import zio.Chunk

import java.util as ju
import java.util.zip.{CRC32, Inflater}
import scala.annotation.tailrec

// Runs the Inflater purely to validate the deflate stream and to compute the CRC32/size that
// CheckTrailerStep needs, but the *output* of this state is the still-compressed input bytes
// it consumed, not the inflated ones.
class Decompress(
  bufferSize: Int
) extends State {

  private val inflater            = new Inflater(true)
  private val crc32: CRC32        = new CRC32
  private val buffer: Array[Byte] = new Array[Byte](bufferSize)
  
  private var pendingInput: Array[Byte] = Array.emptyByteArray
  private var consumedBytes: Int = 0

  private def validateChunk(inflater: Inflater, buffer: Array[Byte]): Unit = {
    @tailrec
    def next(): Unit = {
      val read = inflater.inflate(buffer)
      if (read > 0) crc32.update(buffer, 0, read)
      if (read > 0 && inflater.getRemaining > 0) next()
    }
    if (!inflater.needsInput()) next()
  }

  override def close(): Unit = inflater.end()

  override def feed(chunkBytes: Array[Byte]): (State, Chunk[Byte]) = {
    val input = pendingInput ++ chunkBytes
    inflater.setInput(input)
    validateChunk(inflater, buffer)
    val consumed = input.length - inflater.getRemaining
    val compressedOutput = Chunk.fromArray(ju.Arrays.copyOf(input, consumed))
    val leftover = input.drop(consumed)
    if (inflater.finished()) {
      val newState: CheckTrailerStep = CheckTrailerStep(Array.emptyByteArray, crc32.getValue, inflater.getBytesWritten, consumedBytes + consumed)
      val (state, restOfChunks) = newState.feed(leftover)
      (state, compressedOutput ++ restOfChunks)
    } else {
      consumedBytes += consumed
      pendingInput = leftover
      (this, compressedOutput)
    }
  }
}


