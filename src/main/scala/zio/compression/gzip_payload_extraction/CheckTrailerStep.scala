package zio.compression.gzip_payload_extraction

import State._
import zio.Chunk
import zio.stream.compression.CompressionException

case class CheckTrailerStep(
  acc: Array[Byte],
  expectedCrc32: Long,
  expectedIsize: Long,
  compressedSize: Long
) extends State {

  private def readInt(a: Array[Byte]): Int = u32(a(0), a(1), a(2), a(3))

  override def feed(chunkBytes: Array[Byte]): (State, Chunk[Byte]) = {
    val bytes = acc ++ chunkBytes
    if (bytes.length < 8)
      (CheckTrailerStep(bytes, expectedCrc32, expectedIsize, compressedSize), Chunk.empty) // need more input
    else {
      val (trailerBytes, leftover) = bytes.splitAt(8)
      val crc32                    = readInt(trailerBytes.take(4))
      val isize                    = readInt(trailerBytes.drop(4))
      if (expectedCrc32.toInt != crc32) throw CompressionException("Invalid CRC32")
      else if (expectedIsize.toInt != isize) throw CompressionException("Invalid ISIZE")
      else {
        (this, Chunk.fromArray(leftover))
      }
    }
  }

  override def isInProgress: Boolean = false
}


