package zio.compression.zip

import zio.compression.zip.CompressionMethod.{DEFLATE, STORE}
import zio.stream.ZStream

sealed trait ZipEntry {
  val fileName: String
  val contentStream: ZStream[Any, Throwable, Byte]
  val compressionMethod: CompressionMethod
}

object ZipEntry {
  // uses DataDescriptor - java doesn't like this format
  case class IrregularUncompressedZipEntry(fileName: String, contentStream: ZStream[Any, Throwable, Byte]) extends ZipEntry {
    override val compressionMethod: CompressionMethod = STORE
  }
  // does not use DataDescriptor, crc and length must be known in advance
  case class UncompressedZipEntry(fileName: String, contentStream: ZStream[Any, Throwable, Byte], crc: Int, size: Int) extends ZipEntry {
    override val compressionMethod: CompressionMethod = STORE
  }
  
  case class CompressedZipEntry(fileName: String, contentStream: ZStream[Any, Throwable, Byte]) extends ZipEntry {
    override val compressionMethod: CompressionMethod = DEFLATE
  }
}
