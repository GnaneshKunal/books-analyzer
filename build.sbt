name := "books-analyzer"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

resolvers += "Apache HBase" at "https://repository.apache.org/content/repositories/releases"

resolvers += "Thrift" at "http://people.apache.org/~rawson/repo/"