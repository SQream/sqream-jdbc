name := "(SQream JDBC)"
organization := "com.sqream"
version := "2.9.4"
description := "SQream JDBC driver"
exportJars := true

// Enables publishing to maven repo
publishMavenStyle := true

// Do not append Scala versions to the generated artifacts
crossPaths := false

// This forbids including Scala related libraries into the dependency
autoScalaLibrary := false

// Manifest file additions. See what happens when I don't use this
packageOptions := Seq(ManifestAttributes(
                      ("Implementation-Version", version))))


// library dependencies. (orginization name) % (project name) % (version)
libraryDependencies ++= Seq(
   "com.github.marianobarrios" % "tls-channel" % "0.2.0",
   "junit" % "junit" % "4.12" % "test",  // Test-only dependency
)
