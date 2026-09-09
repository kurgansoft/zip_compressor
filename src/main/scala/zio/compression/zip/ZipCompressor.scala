package zio.compression.zip

import zio.compression.gzip_payload_extraction.GzipPayloadExtractor
import zio.compression.zip.CompressionMethod.{DEFLATE, STORE}
import zio.stream.ZStream
import zio.{Ref, ZIO}

object ZipCompressor {

  private case class Entry(fileName: String, offset: Int = 0, extractionInfo: ExtractionInfo, compressionMethod: CompressionMethod = DEFLATE)

  private case class BookKeeper(entries: List[Entry], sizeOfTheLastEntry: Int = -1) {
    def updateEntryAtIndex(index: Int, updaterFunction: Entry => Entry): BookKeeper = {
      val theUpdatedEntry = updaterFunction(entries(index))
      this.copy(entries = entries.updated(index, theUpdatedEntry))
    }
  }

  def create(list: List[ZipEntry]): ZStream[Any, Throwable, Byte] = ZStream.scoped(
    for {
      bookKeepingRef: Ref[BookKeeper] <- Ref.make(
        BookKeeper(list.map(zipEntry =>
          Entry(
            fileName = zipEntry.fileName,
            extractionInfo = createEmptyExtractionInfo(zipEntry.compressionMethod)
          )
        ))
      )
    } yield firstPart(list, bookKeepingRef) ++ createTocStream(bookKeepingRef)
  ).flatten

  private def createEmptyExtractionInfo(compressionMethod: CompressionMethod): ExtractionInfo = compressionMethod match {
    case DEFLATE => GzipExtractionInfo()
    case STORE => UncompressedExtractionInfo()
  }

  private def createDataDescriptorStream(extractionInfoRef: Ref[ExtractionInfo], dataDescriptorLength: Ref[Int]): ZStream[Any, Throwable, Byte] =
    ZStream.fromZIO(
      for {
        extractionInfo <- extractionInfoRef.get
        dataDescriptorByteArray <- createDataDescriptor(extractionInfoRef).map(_.asByteArray)
        _ <- dataDescriptorLength.set(dataDescriptorByteArray.length)
      } yield dataDescriptorByteArray
    ).flatMap(ZStream.fromIterable(_))

  private def createDataDescriptor(extractionInfoRef: Ref[ExtractionInfo]): ZIO[Any, Nothing, DataDescriptor] =
    for {
      extractionInfo <- extractionInfoRef.get
      dataDescriptor = DataDescriptor(extractionInfo.crc, extractionInfo.compressedSize, extractionInfo.originalSize)
    } yield dataDescriptor

  private def createEmptyStream(bookKeeperRef: Ref[BookKeeper],
                                extractionInfoRef: Ref[ExtractionInfo],
                                index: Int,
                                localFileHeaderLength: Int,
                                dataDescriptorLengthRef: Ref[Int],
                                lastOne: Boolean
                               ): ZStream[Any, Throwable, Byte] =
    ZStream.fromZIO(
      for {
        gzipExtractionInfo <- extractionInfoRef.get
        _ <- bookKeeperRef.update(current =>
          current.updateEntryAtIndex(index, _.copy(
            extractionInfo = gzipExtractionInfo,
            compressionMethod = gzipExtractionInfo.compressionMethod)
          )
        )
        dataDescriptorLength <- dataDescriptorLengthRef.get
        _ <- ZIO.when(!lastOne)(
          bookKeeperRef.update(current => {
            val nextOffset = current.entries(index).offset +
              localFileHeaderLength +
              gzipExtractionInfo.compressedSize +
              dataDescriptorLength
            current.updateEntryAtIndex(index + 1, _.copy(offset = nextOffset))
          })
        )
        _ <- ZIO.when(lastOne)(
          bookKeeperRef.update(_.copy(sizeOfTheLastEntry = localFileHeaderLength +
            gzipExtractionInfo.compressedSize +
            dataDescriptorLength)
          )
        )
      } yield ()
    ).drain

  private def createStreamFromOneFile(
    zipEntry: ZipEntry,
    index: Int,
    bookKeepingRef: Ref[BookKeeper],
    lastOne: Boolean = false
  ): ZStream[Any, Throwable, Byte] = ZStream.scoped(
    for {
      dataDescriptorLengthRef <- Ref.make[Int](-1)
      localFileHeaderAsByteArray = LocalFileHeader(zipEntry.fileName, zipEntry.compressionMethod).asByteArray

      (stream, extractionInfoRef) <- zipEntry match {
        case ZipEntry.UncompressedZipEntry(_, contentStream) => for {
          extractionInfoRef <- Ref.make(UncompressedExtractionInfo())
          stream = contentStream.via(UncompressedPayloadExtractor.createPipeLine(ref = extractionInfoRef))
        } yield (stream, extractionInfoRef.asInstanceOf[Ref[ExtractionInfo]])
        case ZipEntry.CompressedZipEntry(_ ,contentStream) => for {
          extractionInfoRef <- Ref.make(GzipExtractionInfo())
          stream = contentStream.via(GzipPayloadExtractor.createGunzipPipeLine(ref = extractionInfoRef))
        } yield (stream, extractionInfoRef.asInstanceOf[Ref[ExtractionInfo]])
      }
      dataDescriptor <- createDataDescriptor(extractionInfoRef)
     } yield ZStream.fromIterable(localFileHeaderAsByteArray) ++
       stream ++
       createDataDescriptorStream(extractionInfoRef, dataDescriptorLengthRef) ++
       createEmptyStream(bookKeepingRef, extractionInfoRef, index, localFileHeaderAsByteArray.length, dataDescriptorLengthRef, lastOne)
   ).flatten

  private def createTocStream(bookKeeperRef: Ref[BookKeeper]): ZStream[Any, Throwable, Byte] = ZStream.scoped(
    for {
      bookKeeper <- bookKeeperRef.get
      centralDirectoryHeaders = bookKeeper.entries.map(entry =>
        CentralDirectoryHeader(
          entry.fileName,
          entry.extractionInfo.crc,
          entry.extractionInfo.compressedSize,
          entry.extractionInfo.originalSize,
          entry.offset,
          entry.compressionMethod)
      )
      centralDirectoryHeadersBytes = centralDirectoryHeaders.flatMap(_.asByteArray)
      endOfCentralDirectory =
        EndOfCentralDirectory(
          bookKeeper.entries.size,
          centralDirectoryHeadersBytes.size,
          bookKeeper.entries.last.offset + bookKeeper.sizeOfTheLastEntry
        )
    } yield ZStream.fromIterable(centralDirectoryHeadersBytes ++ endOfCentralDirectory.asByteArray)
  ).flatten

  private def firstPart(list: List[ZipEntry], bookKeeperRef: Ref[BookKeeper]): ZStream[Any, Throwable, Byte] = {
    assert(list.nonEmpty)

    val withoutLast = list.take(list.size - 1)

    def createStreamWithIndex(index: Int) = {
      val lastOne = index == list.size - 1
      createStreamFromOneFile(list(index), index, bookKeeperRef, lastOne)
    }

    list.indices
      .map(index => createStreamWithIndex(index))
      .reduce(_ ++ _)
  }
}
