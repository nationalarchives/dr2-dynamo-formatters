import sbtrelease.ReleaseStateTransformations._
import Dependencies._

lazy val scala3Version = "3.3.3"

lazy val releaseSettings = Seq(
  useGpgPinentry := true,
  publishTo := sonatypePublishToBundle.value,
  publishMavenStyle := true,
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    releaseStepCommand("publishSigned"),
    releaseStepCommand("sonatypeBundleRelease"),
    setNextVersion,
    commitNextVersion,
    pushChanges
  ),
  resolvers +=
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  version := (ThisBuild / version).value,
  organization := "uk.gov.nationalarchives",
  organizationName := "National Archives",
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/nationalarchives/dr2-dynamo-formatters"),
      "git@github.com:nationalarchives/dr2-dynamo-formatters.git"
    )
  ),
  developers := List(
    Developer(
      id = "tna-digital-archiving-jenkins",
      name = "TNA Digital Archiving",
      email = "digitalpreservation@nationalarchives.gov.uk",
      url = url("https://github.com/nationalarchives/dr2-dynamo-formatters")
    )
  ),
  description := "Common formatters for parsing Dynamo DB responses to Scala case classes",
  licenses := List("MIT" -> new URI("https://choosealicense.com/licenses/mit/").toURL),
  homepage := Some(url("https://github.com/nationalarchives/dr2-dynamo-formatters"))
)

lazy val commonSettings = Seq(
  scalaVersion := scala3Version,
  libraryDependencies ++= Seq(
    scanamo,
    scalaTest % Test
  ),
  scalacOptions += "-deprecation",
  Test / fork := true,
  Test / envVars := Map("AWS_ACCESS_KEY_ID" -> "test", "AWS_SECRET_ACCESS_KEY" -> "test")
) ++ releaseSettings


lazy val root: Project = project
  .in(file("."))
  .settings(commonSettings)
  .settings(
    name := "dynamo-formatters"
  )


scalacOptions ++= Seq("-Wunused:imports", "-Werror")
