ThisBuild / organization  := "com.sparkx"
ThisBuild / scalaVersion  := "2.12.18"

val sparkVersion = "3.5.0"

val assemblySettings = Seq(
  assemblyPackageScala / assembleArtifact := false,
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "services", _*) => MergeStrategy.concat
    case PathList("META-INF", _*)             => MergeStrategy.discard
    case _                                    => MergeStrategy.first
  }
)

// ── Core plugin ──────────────────────────────────────────────────────────────
lazy val root = (project in file("."))
  .settings(
    name    := "sparkx",
    version := "0.1.0",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided"
    ),
    assemblySettings
  )

// ── Sample / showcase application ────────────────────────────────────────────
lazy val sample = (project in file("sample"))
  .dependsOn(root)
  .settings(
    name    := "sparkx-sample",
    version := "0.1.0",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided"
    ),
    assembly / mainClass := Some("com.sparkx.sample.SparkXDemo"),
    assemblySettings
  )
