name := "sagas"

version := "0.1"

scalaVersion := "2.12.8"

resolvers += Resolver.sonatypeRepo("releases")

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.9")

scalacOptions += "-Ypartial-unification"
libraryDependencies += "org.typelevel" %% "cats-core" % "1.6.0"
libraryDependencies += "org.typelevel" %% "cats-effect" % "1.2.0"

libraryDependencies += "org.scalaz" %% "scalaz-zio" % "1.0-RC3"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"