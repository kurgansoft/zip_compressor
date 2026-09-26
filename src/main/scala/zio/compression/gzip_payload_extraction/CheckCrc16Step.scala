package zio.compression.gzip_payload_extraction

import State._
import zio.Chunk
import zio.stream.compression.CompressionException

case class CheckCrc16Step(
  pastCrc16Bytes: Array[Byte],
  crcValue: Long,
  decompressFn: () => State = null
) extends State {

  override def feed(chunkBytes: Array[Byte]): (State, Chunk[Byte]) = {
    val (crc16Bytes, leftover) = (pastCrc16Bytes ++ chunkBytes).splitAt(2)
    // Unlikely but possible that chunk was 1 byte only, leftover is empty.
    if (crc16Bytes.length < 2) {
      (CheckCrc16Step(crc16Bytes, crcValue, decompressFn), Chunk.empty)
    } else {
      val computedCrc16 = (crcValue & 0xffffL).toInt
      val expectedCrc   = u16(crc16Bytes(0), crc16Bytes(1))
      if (computedCrc16 != expectedCrc) throw CompressionException("Invalid header CRC16")
      else decompressFn().feed(leftover)
    }
  }
}

