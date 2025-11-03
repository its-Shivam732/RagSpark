name := "RagSpark"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.18"

javaOptions ++= Seq(
  "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED"
)

libraryDependencies ++= Seq(
  // --- Apache Spark ---
  "org.apache.spark" %% "spark-core" % "3.5.3" % "provided",
  "org.apache.spark" %% "spark-sql"  % "3.5.3" % "provided",

  // --- Delta Lake ---
  "io.delta" %% "delta-spark" % "3.2.0",

  // --- PDF text extraction ---
  "org.apache.pdfbox" % "pdfbox" % "2.0.30",

  // --- HTTP client ---
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "org.http4s" %% "http4s-ember-server" % "0.23.23",
  "org.http4s" %% "http4s-dsl" % "0.23.23",
  "org.http4s" %% "http4s-circe" % "0.23.23",
  "com.softwaremill.sttp.client3" %% "core"  % "3.9.5",
  "com.softwaremill.sttp.client3" %% "circe" % "3.9.5",
  "io.circe" %% "circe-generic" % "0.14.9",
  "io.circe" %% "circe-parser"  % "0.14.9",

  // --- Lucene (text analysis/search utilities) ---
  "org.apache.lucene" % "lucene-core" % "9.8.0",
  "org.apache.lucene" % "lucene-analysis-common" % "9.8.0",
  "org.apache.lucene" % "lucene-queryparser" % "9.8.0",

  // --- Logging ---
  "org.slf4j" % "slf4j-api" % "2.0.13",
  "ch.qos.logback" % "logback-classic" % "1.5.6",

  // --- Testing Frameworks ---
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  "org.scalatestplus" %% "mockito-4-11" % "3.2.17.0" % Test,
  "org.mockito" %% "mockito-scala" % "1.17.31" % Test,
  "org.mockito" % "mockito-core" % "5.7.0" % Test,

  // Optional alternative mocking library:
   "io.mockk" % "mockk" % "1.13.8" % Test,
 "org.jetbrains.kotlin" % "kotlin-stdlib" % "1.9.22" % Test
)

// --- Assembly plugin configuration ---
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => xs match {
    case "MANIFEST.MF" :: Nil => MergeStrategy.discard
    case "services" :: _      => MergeStrategy.concat
    case _                    => MergeStrategy.discard
  }
  case "reference.conf"     => MergeStrategy.concat
  case "application.conf"   => MergeStrategy.concat
  case x if x.endsWith(".proto") => MergeStrategy.first
  case x if x.contains("module-info") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

assembly / assemblyJarName := s"${name.value}-assembly-${version.value}.jar"
