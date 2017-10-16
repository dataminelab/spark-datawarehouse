scalaVersion := "2.11.8"

scalacOptions ++= Seq("-deprecation")

resolvers += Resolver.sonatypeRepo("releases")
resolvers += "jitpack" at "https://jitpack.io"

// The following doesn't work, added JDBC jar manually to "lib" folder
// See: http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection-with-maven.html
//resolvers +=
//  "redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release"

// grading libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0"
)

libraryDependencies += "com.databricks" %% "spark-xml" % "0.4.1"
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.0.6"

// https://mvnrepository.com/artifact/com.databricks/spark-redshift_2.11
libraryDependencies += "com.databricks" % "spark-redshift_2.11" % "2.0.1"
// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-s3
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.213"
// Temporary fix for: https://github.com/databricks/spark-redshift/issues/315
dependencyOverrides += "com.databricks" % "spark-avro_2.11" % "3.2.0"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.44"

libraryDependencies += "junit" % "junit" % "4.10" % "test"


