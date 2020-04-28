organization := "com.flixdb.cdc"

name := "example"

scalaVersion := "2.13.1"

resolvers +=
  "io.cloudrepo" at "https://flixdb.mycloudrepo.io/public/repositories/cdc"

libraryDependencies += "com.flixdb" %% "cdc" % "0.1"
