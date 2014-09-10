//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

import sbt._, Keys._

import com.twitter.scrooge.ScroogeSBT._

import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin._
import au.com.cba.omnia.uniform.core.version.UniqueVersionPlugin._
import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin._
import au.com.cba.omnia.uniform.thrift.UniformThriftPlugin._
import au.com.cba.omnia.uniform.assembly.UniformAssemblyPlugin._

import au.com.cba.omnia.humbug.HumbugSBT._

object build extends Build {
  val thermometerVersion = "0.3.2-20140922073751-6b24890-CDH5"

  lazy val standardSettings =
    Defaults.coreDefaultSettings ++
    uniformDependencySettings ++
    uniform.docSettings("https://github.com/CommBank/ebenezer") ++ Seq(
      logLevel in sbtassembly.Plugin.AssemblyKeys.assembly := Level.Error
    )

  lazy val all = Project(
    id = "all",
    base = file("."),
    settings =
      standardSettings
        ++ uniform.ghsettings
        ++ Seq(
          publishArtifact := false
        ),
    aggregate = Seq(core, test, hive)
  )

  lazy val core = Project(
    id = "core",
    base = file("core"),
    settings = 
      standardSettings
        ++ uniform.project("ebenezer", "au.com.cba.omnia.ebenezer")
        ++ uniformThriftSettings
        ++ humbugSettings
        ++ Seq(
          libraryDependencies ++=
            depend.hadoop() ++ depend.scalding() ++ depend.scalaz() ++
            depend.parquet() ++ Seq(
              "au.com.cba.omnia" %% "humbug-core" % "0.3.0-20140916025213-f0a7e7f-CDH5",
              "au.com.cba.omnia" %% "thermometer" % thermometerVersion % "test"
            ),
          scroogeThriftSourceFolder in Test <<= (sourceDirectory) { _ / "test" / "thrift" / "scrooge" },
          humbugThriftSourceFolder  in Test <<= (sourceDirectory) { _ / "test" / "thrift" / "humbug" },
          parallelExecution in Test := false
        )
  )

  lazy val test = Project(
    id = "test",
    base = file("test"),
    settings =
      standardSettings
        ++ uniform.project("ebenezer-test", "au.com.cba.omnia.ebenezer.test")
        ++ Seq(
          libraryDependencies ++=
            depend.hadoop() ++ depend.omnia("thermometer", thermometerVersion)
        )
  ).dependsOn(core)

  lazy val hive = Project(
    id = "hive",
    base = file("hive"),
    settings =
      standardSettings
        ++ uniform.project("ebenezer-hive", "au.com.cba.omnia.ebenezer.hive")
        ++ uniformThriftSettings
        ++ Seq(
          libraryDependencies ++=
            depend.hadoop() ++ depend.parquet() ++
            depend.omnia("cascading-hive", "1.5.0-20140922074523-df043ae-CDH5") ++
            Seq(
              "au.com.cba.omnia" %% "thermometer" % thermometerVersion % "test"
            )
        )
  ).dependsOn(core)

  lazy val tools = Project(
    id = "tools",
    base = file("tools"),
    settings =
      standardSettings
        ++ uniform.project("ebenezer-tools", "au.com.cba.omnia.ebenezer.cli")
        ++ uniformThriftSettings
        ++ uniformAssemblySettings
        ++ Seq(
          libraryDependencies ++=
            depend.hadoop() ++ depend.parquet() ++ depend.scalaz() ++ Seq(
              "com.twitter" % "parquet-tools" % "1.4.1" intransitive()
            )
        )
  ).dependsOn(core % "test->test")

  lazy val example = Project(
    id = "example",
    base = file("example"),
    settings =
      standardSettings
        ++ uniform.project("ebenezer-example", "au.com.cba.omnia.ebenezer.example")
        ++ uniformThriftSettings
        ++ uniformAssemblySettings
        ++ Seq(
          parallelExecution in Test := false,
          libraryDependencies ++=
            depend.hadoop() ++ depend.parquet() ++
            depend.omnia("thermometer-hive", thermometerVersion)
        )
  ).dependsOn(hive, test % "test")
}
