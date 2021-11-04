name := "sparkPushDownOperators"

version := "0.1"

scalaVersion := "2.13.7"

idePackagePrefix := Some("org.example.pushdown")


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.14"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test

