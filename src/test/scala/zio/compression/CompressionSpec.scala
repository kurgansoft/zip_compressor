package zio.compression

import zio.Chunk
import zio.compression.zip.ZipCompressor
import zio.compression.zip.ZipEntry.{CompressedZipEntry, UncompressedZipEntry}
import zio.stream.{ZSink, ZStream}
import zio.test._

import java.io.ByteArrayInputStream
import java.util.zip.ZipInputStream

object CompressionSpec extends ZIOSpecDefault {
  
  private case class ZipEntryResult(
      content: Chunk[Byte],
      method: Int
  )

  private def createMapFromZipFile(zipBytes: Chunk[Byte]): Map[String, ZipEntryResult] = {
    val zipInputStream = new ZipInputStream(new ByteArrayInputStream(zipBytes.toArray))
    val entries = scala.collection.mutable.Map.empty[String, ZipEntryResult]
    var entry = zipInputStream.getNextEntry

    while (entry != null) {
      entries += entry.getName -> ZipEntryResult(
        content = Chunk.fromArray(zipInputStream.readAllBytes()),
        method = entry.getMethod
      )
      zipInputStream.closeEntry()
      entry = zipInputStream.getNextEntry
    }

    entries.toMap
  }

  private def calculateCRC(bytes: Chunk[Byte]): Int = {
    val crc = new java.util.zip.CRC32()
    crc.update(bytes.toArray)
    (crc.getValue & 0xFFFFFFFFL).toInt
  }

  def spec = suite("CompressionSpec")(
    test("zip archive from three gzipped entry") {
      val libreFranklinStream = ZStream.fromResource("google_fonts_compressed/libre_franklin.css.gz")
      val notoSansStream = ZStream.fromResource("google_fonts_compressed/noto_sans.css.gz")
      val notoSerifStream = ZStream.fromResource("google_fonts_compressed/noto_serif.css.gz")

      for {
        expectedLibreFranklin <- ZStream.fromResource("google_fonts_uncompressed/libre_franklin.css").runCollect
        expectedNotoSans <- ZStream.fromResource("google_fonts_uncompressed/noto_sans.css").runCollect
        expectedNotoSerif <- ZStream.fromResource("google_fonts_uncompressed/noto_serif.css").runCollect
        zipBytes <- ZipCompressor
          .create(List(
            CompressedZipEntry("noto_sans.css", notoSansStream),
            CompressedZipEntry("noto_serif.css", notoSerifStream),
            CompressedZipEntry("libre_franklin.css", libreFranklinStream),
          ))
          .run(ZSink.collectAll[Byte])

        results = createMapFromZipFile(zipBytes)
      } yield assertTrue(results == Map(
        "noto_sans.css" -> ZipEntryResult(expectedNotoSans, 8),
        "noto_serif.css" -> ZipEntryResult(expectedNotoSerif, 8),
        "libre_franklin.css" -> ZipEntryResult(expectedLibreFranklin, 8),
      ))
    },
    test("zip archive from three uncompressed entry") {
      def samplePdfStream() = ZStream.fromResource("sample.pdf")
      def samplePngStream() = ZStream.fromResource("sample.png")
      def sampleTxtStream() = ZStream.fromResource("sample.txt")

      for {
        samplePdfBytes <- ZStream.fromResource("sample.pdf").run(ZSink.collectAll)
        samplePdfBytesCrc = calculateCRC(samplePdfBytes)

        samplePngBytes <- ZStream.fromResource("sample.png").run(ZSink.collectAll)
        samplePngBytesCrc = calculateCRC(samplePngBytes)

        sampleTxtBytes <- ZStream.fromResource("sample.txt").run(ZSink.collectAll)
        sampleTxtBytesCrc = calculateCRC(sampleTxtBytes)

        expected1 <- samplePdfStream().runCollect
        expected2 <- samplePngStream().runCollect
        expected3 <- sampleTxtStream().runCollect
        zipBytes <- ZipCompressor
          .create(List(
            UncompressedZipEntry("sample.pdf", samplePdfStream(), samplePdfBytesCrc, samplePdfBytes.size),
            UncompressedZipEntry("sample.png", samplePngStream(), samplePngBytesCrc, samplePngBytes.size),
            UncompressedZipEntry("sample.txt", sampleTxtStream(), sampleTxtBytesCrc, sampleTxtBytes.size),
          ))
          .run(ZSink.collectAll[Byte])

        results = createMapFromZipFile(zipBytes)
      } yield assertTrue(results == Map(
        "sample.pdf" -> ZipEntryResult(expected1, 0),
        "sample.png" -> ZipEntryResult(expected2, 0),
        "sample.txt" -> ZipEntryResult(expected3, 0),
      ))
    }

  )
}