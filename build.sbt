name := "history-server"

organization := "com.github.lightcopy"

// scala version for tests
scalaVersion := "2.11.7"

// Compile dependencies
libraryDependencies ++= Seq(
  "org.glassfish.jersey.containers" % "jersey-container-servlet" % "2.25.1",
  "org.glassfish.jersey.containers" % "jersey-container-grizzly2-http" % "2.25.1",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  // Hadoop dependencies
  ("org.apache.hadoop" % "hadoop-client" % "2.7.0").
    exclude("aopalliance", "aopalliance").
    exclude("com.sun.jersey", "jersey-core").
    exclude("com.sun.jersey", "jersey-json").
    exclude("com.sun.jersey", "jersey-server").
    exclude("commons-beanutils", "commons-beanutils").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-httpclient", "commons-httpclient").
    exclude("javax.inject", "javax.inject").
    exclude("junit", "junit").
    exclude("org.apache.hadoop", "hadoop-yarn-api").
    exclude("org.mortbay.jetty", "jetty").
    exclude("org.mortbay.jetty", "jetty").
    exclude("org.mortbay.jetty", "jetty-util").
    exclude("org.mortbay.jetty", "servlet-api"),
  // MongoDB dependencies
  "org.mongodb" % "mongodb-driver" % "3.2.2"
)

// Test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test"
)

javacOptions in ThisBuild ++= Seq("-Xlint:unchecked")
scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation", "-feature")

// Display full-length stacktraces from ScalaTest
testOptions in Test += Tests.Argument("-oF")
testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "+q")

parallelExecution in Test := false

// Skip tests during assembly
test in assembly := {}
// Exclude scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  // There is an issue with hadoop-common and hadoop-hdfs declaring FileSystem, and discarding
  // one of the implementations of those - instead we just keep distinct declarations
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") =>
    MergeStrategy.filterDistinctLines
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.first
  // Exclude all static content from dependencies
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last.endsWith(".html") => MergeStrategy.discard
  case PathList(ps @ _*) if ps.exists(_ == "assets") => MergeStrategy.discard
  case PathList(ps @ _*) if ps.exists(_ == "webapps") => MergeStrategy.discard
  // Exclude any contribs, licences, notices, readme from dependencies
  case PathList("contribs", xs @ _*) => MergeStrategy.discard
  case PathList("license", xs @ _*) => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last.toUpperCase.startsWith("LICENSE") => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last.toUpperCase.startsWith("NOTICE") => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last.toUpperCase.startsWith("README_") => MergeStrategy.discard
  case other =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(other)
}
