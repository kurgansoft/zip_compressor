package zio.compression.zip

import zio.compression.zip.CompressionMethod.{DEFLATE, STORE}

case class LocalFileHeader(
  filename: String,
  compressionMethod: CompressionMethod = DEFLATE,
  crc32: Int = 0,
  compressedSize: Int = 0,
  uncompressedSize: Int = 0,
  dataDescriptorUsed: Boolean = true
) {
  lazy val asByteArray: Array[Byte] = {
    import java.nio.{ByteBuffer, ByteOrder}

    val filenameBytes = filename.getBytes("UTF-8")
    val filenameLength = filenameBytes.length.toShort
    val generalPurposeFlags: Short = if (dataDescriptorUsed) 0x0008 else 0

    // Total: 30 bytes fixed header + filename length
    val buffer = ByteBuffer.allocate(30 + filenameLength)
    buffer.order(ByteOrder.LITTLE_ENDIAN)

    buffer.putInt(0x04034b50)        // signature
    buffer.putShort(20)              // version needed (2.0)
    buffer.putShort(generalPurposeFlags)
    buffer.putShort(compressionMethod.value)
    buffer.putShort(0)               // last mod time
    buffer.putShort(0)               // last mod date
    buffer.putInt(crc32)             // CRC-32 (0 for Data Descriptor)
    buffer.putInt(compressedSize)    // compressed size (0 for Data Descriptor)
    buffer.putInt(uncompressedSize)  // uncompressed size (0 for Data Descriptor)
    buffer.putShort(filenameLength)  // filename length
    buffer.putShort(0)               // extra field length
    buffer.put(filenameBytes)        // filename bytes

    buffer.array()
  }
}

object LocalFileHeader {
  def createLocalFileHeaderForStoredEntry(fileName: String, crc32: Int, size: Int): LocalFileHeader =
    LocalFileHeader(fileName, STORE, crc32, compressedSize = size, uncompressedSize = size, dataDescriptorUsed = false)
}
