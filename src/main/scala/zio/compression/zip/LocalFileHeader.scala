package zio.compression.zip

import zio.compression.zip.CompressionMethod.DEFLATE

case class LocalFileHeader(filename: String, compressionMethod: CompressionMethod = DEFLATE) {
  lazy val asByteArray: Array[Byte] = {
    import java.nio.{ByteBuffer, ByteOrder}

    val filenameBytes = filename.getBytes("UTF-8")
    val filenameLength = filenameBytes.length.toShort

    // Total: 30 bytes fixed header + filename length
    val buffer = ByteBuffer.allocate(30 + filenameLength)
    buffer.order(ByteOrder.LITTLE_ENDIAN)

    buffer.putInt(0x04034b50)        // signature
    buffer.putShort(20)              // version needed (2.0)
    buffer.putShort(0x0008)          // general purpose flags (bit 3 = Data Descriptor)
    buffer.putShort(compressionMethod.value)
    buffer.putShort(0)               // last mod time
    buffer.putShort(0)               // last mod date
    buffer.putInt(0)                 // CRC-32 (0 for Data Descriptor)
    buffer.putInt(0)                 // compressed size (0 for Data Descriptor)
    buffer.putInt(0)                 // uncompressed size (0 for Data Descriptor)
    buffer.putShort(filenameLength)  // filename length
    buffer.putShort(0)               // extra field length
    buffer.put(filenameBytes)        // filename bytes

    buffer.array()
  }
}
