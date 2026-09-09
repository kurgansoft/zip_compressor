package zio.compression.zip

import zio.compression.zip.CompressionMethod
import zio.compression.zip.CompressionMethod.{DEFLATE, STORE}

sealed trait ExtractionInfo {
  val crc: Int
  val originalSize: Int
  val compressedSize: Int
  val compressionMethod: CompressionMethod
}

case class GzipExtractionInfo(
                               crc: Int = 0,
                               originalSize: Int = 0,
                               compressedSize: Int = 0
                             ) extends ExtractionInfo {
  override val compressionMethod: CompressionMethod = DEFLATE
}

case class UncompressedExtractionInfo(
                               crc: Int = 0,
                               size: Int = 0,
                             ) extends ExtractionInfo {
  override val compressedSize: Int = size
  override val originalSize: Int = size
  override val compressionMethod: CompressionMethod = STORE
}
