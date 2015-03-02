lazy val root = (project in file (".")).
  settings(
    name := "Simple Project",
    version := "1.0",
    scalaVersion := "2.10.4",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.1" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.2.1" % "provided",
    libraryDependencies += "joda-time" % "joda-time" % "2.7",
    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.0-alpha2",
    libraryDependencies += "org.apache.cassandra" % "cassandra-thrift" % "2.1.3",
    libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.4",
    assemblyJarName in assembly := "something.jar",
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )
