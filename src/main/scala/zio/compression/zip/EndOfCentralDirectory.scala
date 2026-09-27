package zio.compression.zip

case class EndOfCentralDirectory(
                                  totalEntries: Int,
                                  centralDirectorySize: Int,
                                  centralDirectoryOffset: Int
                                ) {
  lazy val asByteArray: Array[Byte] = {
    import java.nio.{ByteBuffer, ByteOrder}

    val buffer = ByteBuffer.allocate(22)
    buffer.order(ByteOrder.LITTLE_ENDIAN)

    buffer.putInt(0x06054b50)               // signature
    buffer.putShort(0)                      // disk number
    buffer.putShort(0)                      // disk with central directory
    buffer.putShort(totalEntries.toShort)   // entries on this disk
    buffer.putShort(totalEntries.toShort)   // total entries
    buffer.putInt(centralDirectorySize)     // size of central directory
    buffer.putInt(centralDirectoryOffset)   // offset of central directory
    buffer.putShort(0)                      // comment length

    buffer.array()
  }
}

