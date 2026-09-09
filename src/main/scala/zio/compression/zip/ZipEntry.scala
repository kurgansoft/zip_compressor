package zio.compression.zip

import zio.compression.zip.CompressionMethod.{DEFLATE, STORE}
import zio.stream.ZStream

sealed trait ZipEntry {
  val fileName: String
  val contentStream: ZStream[Any, Throwable, Byte]
  val compressionMethod: CompressionMethod
}

object ZipEntry {
  case class UncompressedZipEntry(fileName: String, contentStream: ZStream[Any, Throwable, Byte]) extends ZipEntry {
    override val compressionMethod: CompressionMethod = STORE
  }
  case class CompressedZipEntry(fileName: String, contentStream: ZStream[Any, Throwable, Byte]) extends ZipEntry {
    override val compressionMethod: CompressionMethod = DEFLATE
  }
}
