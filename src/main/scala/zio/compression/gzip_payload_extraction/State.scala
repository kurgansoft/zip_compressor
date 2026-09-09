package zio.compression.gzip_payload_extraction

import zio.Chunk

trait State {
  def close(): Unit = ()
  def feed(chunkBytes: Array[Byte]): (State, Chunk[Byte])
  def isInProgress: Boolean = true
}

object State {
  val fixedHeaderLength = 10

  def u8(b: Byte): Int = b & 0xff

  def u16(b1: Byte, b2: Byte): Int = u8(b1) | (u8(b2) << 8)

  def u32(b1: Byte, b2: Byte, b3: Byte, b4: Byte): Int = u16(b1, b2) | (u16(b3, b4) << 16)
}


