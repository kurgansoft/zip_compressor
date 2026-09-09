package zio.compression.gzip_payload_extraction

import zio.Chunk

import java.util as ju
import java.util.zip.{CRC32, Inflater}
import scala.annotation.tailrec

// Runs the Inflater purely to validate the deflate stream and to compute the CRC32/size that
// CheckTrailerStep needs, but the *output* of this state is the still-compressed input bytes
// it consumed, not the inflated ones.
case class Decompress(
  bufferSize: Int,
  consumedBytes: Int = 0
) extends State {

  private val inflater            = new Inflater(true)
  private val crc32: CRC32        = new CRC32
  private val buffer: Array[Byte] = new Array[Byte](bufferSize)

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
    inflater.setInput(chunkBytes)
    validateChunk(inflater, buffer)
    val consumed         = chunkBytes.length - inflater.getRemaining
    val compressedOutput = Chunk.fromArray(ju.Arrays.copyOf(chunkBytes, consumed))
    if (inflater.finished()) {
      val leftover = chunkBytes.takeRight(inflater.getRemaining)
      val newState: CheckTrailerStep = CheckTrailerStep(Array.emptyByteArray, crc32.getValue, inflater.getBytesWritten, consumedBytes + consumed)
      val (state, restOfChunks) =
        newState.feed(leftover)
      (state, compressedOutput ++ restOfChunks)
    } else (this.copy(consumedBytes = consumedBytes + consumed), compressedOutput)
  }
}


