package zio.compression.zip

import zio.compression.zip.CompressionMethod.DEFLATE

case class CentralDirectoryHeader(
                                   filename: String,
                                   crc32: Long,
                                   compressedSize: Long,
                                   uncompressedSize: Long,
                                   offsetOfLocalHeader: Long,
                                   compressionMethod: CompressionMethod = DEFLATE,
                                   dataDescriptorUsed: Boolean = true
                                 ) {
  lazy val asByteArray: Array[Byte] = {
    import java.nio.{ByteBuffer, ByteOrder}

    val filenameBytes = filename.getBytes("UTF-8")
    val filenameLength = filenameBytes.length.toShort
    val generalPurposeFlags: Short = if (dataDescriptorUsed) 0x0008 else 0

    val buffer = ByteBuffer.allocate(46 + filenameLength)
    buffer.order(ByteOrder.LITTLE_ENDIAN)

    buffer.putInt(0x02014b50)                 // signature
    buffer.putShort(20)                       // version made by
    buffer.putShort(20)                       // version needed to extract
    buffer.putShort(generalPurposeFlags)      // general purpose flags
    buffer.putShort(compressionMethod.value)  // compression method
    buffer.putShort(0)                        // last mod time
    buffer.putShort(0)                        // last mod date
    buffer.putInt(crc32.toInt)                // CRC-32
    buffer.putInt(compressedSize.toInt)       // compressed size
    buffer.putInt(uncompressedSize.toInt)     // uncompressed size
    buffer.putShort(filenameLength)           // filename length
    buffer.putShort(0)                        // extra field length
    buffer.putShort(0)                        // file comment length
    buffer.putShort(0)                        // disk number start
    buffer.putShort(0)                        // internal file attributes
    buffer.putInt(0)                          // external file attributes
    buffer.putInt(offsetOfLocalHeader.toInt)  // relative offset of local header
    buffer.put(filenameBytes)                 // filename

    buffer.array()
  }
}