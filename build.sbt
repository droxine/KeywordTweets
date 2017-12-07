name := "KeywordTweets"

version := "0.1"

scalaVersion := "2.11.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0" exclude("io.netty","netty") exclude("commons-net","commons-net")

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"

libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.0"

libraryDependencies += "com.github.catalystcode" %% "streaming-facebook" % "0.0.3"

libraryDependencies += "org.ddahl" %% "rscala" % "2.5.0"

//retrieveManaged := true

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "commons", "collections", xs @ _*) =>
    MergeStrategy.last
  case PathList("mime.types") =>
    MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

mainClass in Compile := Some("RScalaDemo")