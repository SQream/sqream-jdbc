name :=         "SQream JDBC"
organization := "com.sqream"
version :=      "2.9.4"
description :=  "SQream JDBC driver"

// Compilation parameters
//exportJars := true             
//publishMavenStyle := true
//scalaVersion := "2.12.8"
crossPaths :=       false  
autoScalaLibrary := false        // adds the scala library as a dependency

// library dependencies
libraryDependencies ++= Seq(
   "com.github.marianobarrios" % "tls-channel" % "0.2.0",
   "junit" % "junit" % "4.12" % Test,  
   "com.novocode" % "junit-interface" % "0.11" % Test
)

// Assembly (fat jar) parameters
assemblyJarName := "SqreamJDBC.jar"
test in assembly := {}
//assemblyOutputPath := file("..")

packageOptions in assembly := Seq(Package.ManifestAttributes(("Implementation-Version", version.value)))

testOptions += Tests.Argument(TestFrameworks.JUnit)

/*
assemblyMergeStrategy in assembly := {
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case x => MergeStrategy.first
}
*/