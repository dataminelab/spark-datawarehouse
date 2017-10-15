scalaVersion := "2.11.8"

scalacOptions ++= Seq("-deprecation")

resolvers += Resolver.sonatypeRepo("releases")
resolvers += "jitpack" at "https://jitpack.io"

// grading libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0"
)

libraryDependencies += "com.databricks" %% "spark-xml" % "0.4.1"
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.0.6"

// https://mvnrepository.com/artifact/com.databricks/spark-redshift_2.11
libraryDependencies += "com.databricks" % "spark-redshift_2.11" % "2.0.1"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.44"

libraryDependencies += "junit" % "junit" % "4.10" % "test"

