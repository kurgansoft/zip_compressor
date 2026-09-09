package zio.compression.zip

case class DataDescriptor(
  crc32: Int,
  compressedSize: Long,
  uncompressedSize: Long
) {
  lazy val asByteArray: Array[Byte] = {
    import java.nio.{ByteBuffer, ByteOrder}
    
    val buffer = ByteBuffer.allocate(16)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    
    buffer.putInt(0x08074b50)              // signature
//    buffer.putInt((crc32 & 0xFFFFFFFFL).toInt)           // CRC-32
    buffer.putInt(crc32)
    buffer.putInt((compressedSize & 0xFFFFFFFFL).toInt)  // compressed size
    buffer.putInt((uncompressedSize & 0xFFFFFFFFL).toInt) // uncompressed size

    buffer.array()
  }
}