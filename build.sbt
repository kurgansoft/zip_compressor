val scala2Version = "2.13.18"
val scala3Version = "3.8.4"

val zioVersion = "2.1.26"

lazy val root = (project in file("."))
  .settings(
    name := "zip-compressor",
    scalaVersion := scala3Version,
    crossScalaVersions := Seq(scala2Version, scala3Version),
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion,
      "dev.zio" %% "zio-streams" % zioVersion,

      "dev.zio" %% "zio-test" % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
