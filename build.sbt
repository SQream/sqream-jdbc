name :=         "SQream JDBC"
organization := "com.sqream"
version :=      "2.9.4"
description :=  "SQream JDBC driver"

// Compilation parameters
//exportJars := true              // exports a thin jar we don't need
//publishMavenStyle := true
//scalaVersion := "2.12.8"
crossPaths :=       false  
autoScalaLibrary := false        // adds the scala library as a dependency

// library dependencies. (orginization name) % (project name) % (version)
libraryDependencies ++= Seq(
   "com.github.marianobarrios" % "tls-channel" % "0.2.0",
   "junit" % "junit" % "4.12" % "test",  //Test-only dependency
   "com.novocode" % "junit-interface" % "0.11" % "test"
)

// Assembly (fat jar) parameters
assemblyJarName := "SqreamJDBC.jar"
test in assembly := {}
//assemblyOutputPath := file("..")

// Manifest file additions. See what happens when I don't use this
packageOptions in assembly := Seq(Package.ManifestAttributes(("Implementation-Version", "2.9.4")))

/*
assemblyMergeStrategy in assembly := {
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case x => MergeStrategy.first
}
*/