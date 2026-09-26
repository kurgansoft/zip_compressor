package zio.compression.gzip_payload_extraction

import State._
import zio.Chunk
import zio.stream.compression.CompressionException

import java.util.zip.CRC32

case class ParseHeaderStep(
  acc: Array[Byte],
  crc32: CRC32,
  optionalInfo: Option[Int] = None,
  nextStepFn: (Array[Byte], Boolean, CRC32, Boolean, Int) => State = null
) extends State {

  // TODO: If whole input is shorter than fixed header, not output is produced and no error is signaled. Is it ok?
  override def feed(chunkBytes: Array[Byte]): (State, Chunk[Byte]) = {

    val bytes = acc ++ chunkBytes
    if (bytes.length < fixedHeaderLength) (ParseHeaderStep(bytes, crc32, optionalInfo, nextStepFn), Chunk.empty)
    else {
      val (header, leftover) = bytes.splitAt(fixedHeaderLength)
      crc32.update(header)
      if (u8(header(0)) != 31 || u8(header(1)) != 139) throw CompressionException("Invalid GZIP header")
      else if (header(2) != 8)
        throw CompressionException(s"Only deflate (8) compression method is supported, present: ${header(2)}")
      else {
        val flags           = header(3) & 0xff
        val checkCrc16      = (flags & 2) > 0
        val hasExtra        = (flags & 4) > 0
        val skipFileName    = (flags & 8) > 0
        val skipFileComment = (flags & 16) > 0
        val commentsToSkip  = (if (skipFileName) 1 else 0) + (if (skipFileComment) 1 else 0)
        nextStepFn(header, checkCrc16, crc32, hasExtra, commentsToSkip).feed(leftover)
      }
    }
  }

  override def isInProgress: Boolean = acc.nonEmpty
}

