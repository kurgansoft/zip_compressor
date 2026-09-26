package zio.compression.gzip_payload_extraction

// Adapted from zio.stream.compression.Gunzipper (zio-streams).

import zio._
import zio.compression.zip.GzipExtractionInfo
import zio.stream.compression._
import zio.stream.{ZChannel, ZPipeline}

import scala.util.{Failure, Success, Try}

/**
 * Performs few steps of parsing header, then validates the compressed body against the
 * trailer's CRC32/ISIZE (by decompressing internally), but emits the still-compressed body
 * bytes rather than the decompressed content — i.e. it strips the gzip header and trailer
 * while passing the deflate-compressed payload through unchanged. With reasonably chosen
 * bufferSize there shouldn't occur many concatenation of arrays.
 */
class GzipPayloadExtractor private(bufferSize: Int) {

  import java.util.zip.CRC32

  implicit private def convert(state: State, chunk: Chunk[Byte]): (State, Chunk[Byte], Option[Long]) = (state, chunk, None)

  private var state: State = createInitialState()

  private def createInitialState(): State = {
    ParseHeaderStep(Array.emptyByteArray, new CRC32(), None, nextStepFn)
  }

  private def nextStepFn(
                         acc: Array[Byte],
                         checkCrc16: Boolean,
                         crc32: CRC32,
                         parseExtra: Boolean,
                         commentsToSkip: Int
                       ): State =
    if (parseExtra) ParseExtraStep(acc, crc32, checkCrc16, commentsToSkip, nextStepFn)
    else if (commentsToSkip > 0) SkipCommentsStep(checkCrc16, crc32, commentsToSkip, nextStepFn)
    else if (checkCrc16) CheckCrc16Step(Array.emptyByteArray, crc32.getValue, createDecompress)
    else createDecompress()

  private def createDecompress(): State = {
    new Decompress(bufferSize)
  }

  def close(): Unit = state.close()

  def onChunk(c: Chunk[Byte], ref: Ref[GzipExtractionInfo])(implicit trace: Trace): ZIO[Any, CompressionException, Chunk[Byte]] = {
    Try(state.feed(c.toArray)) match {
      case Failure(compressionException: CompressionException) =>
        ZIO.fail(compressionException)
      case Failure(exception: Exception) =>
        ZIO.fail(CompressionException(exception))
      case Failure(throwable: Throwable) =>
        throw throwable
      case Success((newState, output)) => {
        state = newState
        val extractionInfo = state match {
          case CheckTrailerStep(_, crc, expectedIsize, compressedSize) =>
            val crcAsInt: Int = (crc & 0xFFFFFFFFL).toInt
            val iSizeAsInt: Int = (expectedIsize & 0xFFFFFFFFL).toInt
            val compressedSizeAsInt: Int = (compressedSize & 0xFFFFFFFFL).toInt
            Some(GzipExtractionInfo(crcAsInt, iSizeAsInt, compressedSizeAsInt))
          case _ => None
        }
        for {
          _ <- ZIO.when(extractionInfo.nonEmpty)(ref.set(extractionInfo.get))
        } yield output
      }
    }
  }

  def onNone(implicit trace: Trace): ZIO[Any, CompressionException, Chunk[Byte]] =
    if (state.isInProgress) ZIO.fail(CompressionException("Stream closed before completion."))
    else Exit.emptyChunk


}

object GzipPayloadExtractor {

  private def make(bufferSize: Int)(implicit trace: Trace): ZIO[Any, Nothing, GzipPayloadExtractor] =
    ZIO.succeed(new GzipPayloadExtractor(bufferSize))

  private def makeGunzipper[Done](
                           bufferSize: Int = 64 * 1024,
                           ref: Ref[GzipExtractionInfo]
                         )(implicit
                           trace: Trace
                         ): ZChannel[Any, CompressionException, Chunk[Byte], Done, CompressionException, Chunk[Byte], Done] =
    ZChannel.unwrapScoped {
      ZIO
        .acquireRelease(
          make(bufferSize)
        )(gunzipper => ZIO.succeed(gunzipper.close()))
        .map {
          case gunzipper => {
            lazy val loop
            : ZChannel[Any, CompressionException, Chunk[Byte], Done, CompressionException, Chunk[Byte], Done] =
              ZChannel.readWithCause(
                chunk =>
                  ZChannel.fromZIO {
                    gunzipper.onChunk(chunk, ref)
                  }.flatMap(chunk => ZChannel.write(chunk) *> loop),
                ZChannel.refailCause,
                done =>
                  ZChannel.fromZIO {
                    gunzipper.onNone
                  }.flatMap(chunk => ZChannel.write(chunk).as(done))
              )

            loop
          }
        }
    }

  def createGunzipPipeLine(bufferSize: Int = 64 * 1024, ref: Ref[GzipExtractionInfo])(implicit trace: Trace): ZPipeline[Any, CompressionException, Byte, Byte] =
    ZPipeline.fromChannel(
      makeGunzipper(bufferSize, ref)
    )
  
}