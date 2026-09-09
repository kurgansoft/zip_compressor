package zio.compression.gzip_payload_extraction

import zio.Chunk

import java.util.zip.CRC32

case class SkipCommentsStep(
  checkCrc16: Boolean,
  crc32: CRC32,
  commentsToSkip: Int,
  nextStepFn: (Array[Byte], Boolean, CRC32, Boolean, Int) => State = null
) extends State {

  override def feed(chunkBytes: Array[Byte]): (State, Chunk[Byte]) = {
    val idx               = chunkBytes.indexOf(0)
    val (upTo0, leftover) = if (idx == -1) (chunkBytes, Array.emptyByteArray) else chunkBytes.splitAt(idx + 1)
    crc32.update(upTo0)
    nextStepFn(Array.emptyByteArray, checkCrc16, crc32, false, commentsToSkip - 1).feed(leftover)
  }
}

