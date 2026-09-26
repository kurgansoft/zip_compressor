package zio.compression.gzip_payload_extraction

import State._
import zio.Chunk

import java.util.zip.CRC32

case class ParseExtraStep(
  acc: Array[Byte],
  crc32: CRC32,
  checkCrc16: Boolean,
  commentsToSkip: Int,
  nextStepFn: (Array[Byte], Boolean, CRC32, Boolean, Int) => State = null
) extends State {

  override def feed(chunkBytes: Array[Byte]): (State, Chunk[Byte]) = {
    val bytes = acc ++ chunkBytes
    if (bytes.length < 12) {
      (ParseExtraStep(bytes, crc32, checkCrc16, commentsToSkip, nextStepFn), Chunk.empty)
    } else {
      val xlenLenght            = 2
      val extraBytes: Int       = u16(bytes(fixedHeaderLength), bytes(fixedHeaderLength + 1))
      val headerWithExtraLength = fixedHeaderLength + xlenLenght + extraBytes
      if (bytes.length < headerWithExtraLength)
        (ParseExtraStep(bytes, crc32, checkCrc16, commentsToSkip, nextStepFn), Chunk.empty)
      else {
        val (headerWithExtra, leftover) = bytes.splitAt(headerWithExtraLength)
        crc32.update(headerWithExtra.drop(fixedHeaderLength))
        nextStepFn(headerWithExtra, checkCrc16, crc32, false, commentsToSkip).feed(leftover)
      }
    }
  }
}

