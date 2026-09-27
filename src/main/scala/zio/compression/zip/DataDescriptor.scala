package zio.compression.zip

case class DataDescriptor(
  crc32: Int,
  compressedSize: Int,
  uncompressedSize: Int
) {
  lazy val asByteArray: Array[Byte] = {
    import java.nio.{ByteBuffer, ByteOrder}
    
    val buffer = ByteBuffer.allocate(16)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    
    buffer.putInt(0x08074b50)              // signature
    buffer.putInt(crc32)
    buffer.putInt(compressedSize)  // compressed size
    buffer.putInt(uncompressedSize) // uncompressed size

    buffer.array()
  }
}