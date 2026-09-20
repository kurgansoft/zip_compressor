ThisBuild / scalaVersion := "3.8.4"

val zioVersion = "2.1.26"

lazy val root = (project in file("."))
  .settings(
    name := "zip-compressor",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion,
      "dev.zio" %% "zio-streams" % zioVersion,

      "dev.zio" %% "zio-test" % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
