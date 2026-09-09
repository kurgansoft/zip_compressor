package zio.compression.zip

sealed trait CompressionMethod {
  val value: Short
}

object CompressionMethod {
  case object STORE extends CompressionMethod {
    override val value: Short = 0
  }

  case object DEFLATE extends CompressionMethod {
    override val value: Short = 8
  }
}