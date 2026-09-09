package zio.compression

import zio.compression.zip.ZipCompressor
import zio.compression.zip.ZipEntry.CompressedZipEntry
import zio.stream.{ZSink, ZStream}
import zio.test.*

import java.io.ByteArrayInputStream
import java.util.zip.ZipInputStream
import zio.Chunk

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

  def spec = suite("CompressionSpec")(
    test("creates a ZIP archive in memory") {
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
    }
  )
}