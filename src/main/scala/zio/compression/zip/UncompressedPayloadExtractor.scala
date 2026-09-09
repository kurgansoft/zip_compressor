package zio.compression.zip

import zio.stream.ZPipeline
import zio.{Chunk, Ref, ZIO}

import java.io.IOException
import java.util.zip.CRC32

object UncompressedPayloadExtractor {
  def createPipeLine(bufferSize: Int = 64 * 1024, ref: Ref[UncompressedExtractionInfo]): ZPipeline[Any, IOException, Byte, Byte] = {
    val crc = new CRC32()
    ZPipeline.fromPush {
      for {
        sizeRef <- Ref.make(0L)
      } yield {
        (input: Option[Chunk[Byte]]) =>
          input match {
            case Some(chunk) =>
              if (chunk.isEmpty) {
                ZIO.succeed(chunk)
              } else {
                for {
                  _ <- sizeRef.update(_ + chunk.length)
                  _ = crc.update(chunk.toArray)
                } yield chunk
              }

            case None =>
              // End of stream: finalize descriptor into the Ref
              for {
                size <- sizeRef.get
                crcAsInt: Int = (crc.getValue & 0xFFFFFFFFL).toInt
                iSizeAsInt: Int = (size & 0xFFFFFFFFL).toInt

                _ <- ref.set(
                  UncompressedExtractionInfo(
                    crc = crcAsInt,
                    size = iSizeAsInt,
                  )
                )
              } yield Chunk.empty
          }
      }
    }
  }
}
