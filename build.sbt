ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.ridesim"
ThisBuild / version      := "0.1.0"

val sparkVersion = "3.5.1"

lazy val root = (project in file("."))
  .settings(
    name := "spark-streaming-alerts",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"           % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql"            % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % Provided,
      "org.scalatest"    %% "scalatest"            % "3.2.18"     % Test
    ),
    run / fork := true,
    run / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    ),
    Test / fork := true,
    Test / javaOptions ++= (run / javaOptions).value,
    Test / parallelExecution := false,
    assembly / assemblyJarName := s"${name.value}-assembly.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) =>
        xs.map(_.toLowerCase) match {
          case "manifest.mf" :: Nil                   => MergeStrategy.discard
          case "index.list" :: Nil                    => MergeStrategy.discard
          case "dependencies" :: Nil                  => MergeStrategy.discard
          case ps if ps.last.endsWith(".sf")          => MergeStrategy.discard
          case ps if ps.last.endsWith(".dsa")         => MergeStrategy.discard
          case ps if ps.last.endsWith(".rsa")         => MergeStrategy.discard
          case "services" :: _                        => MergeStrategy.concat
          case _                                       => MergeStrategy.first
        }
      case "reference.conf"   => MergeStrategy.concat
      case "application.conf" => MergeStrategy.concat
      case _                  => MergeStrategy.first
    }
  )
