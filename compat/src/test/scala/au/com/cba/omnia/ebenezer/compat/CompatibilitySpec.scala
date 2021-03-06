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

package au.com.cba.omnia.ebenezer.compat

import au.com.cba.omnia.thermometer.core._, Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids._
import au.com.cba.omnia.thermometer.hive.HiveSupport

import au.com.cba.omnia.ebenezer.test.ParquetThermometerRecordReader

object CompatibilitySpec extends ThermometerSpec with HiveSupport { def is = sequential ^ s2"""
Internal compatibility tests for non collection types
=====================================================

  Can write using MR and read using MR     ${internal.readMRWriteMR}
  Can write using MR and read using Hive   ${internal.writeMRReadHive}
  Can write using Hive and read using MR   ${internal.writeHiveReadMR}
  Can write using Hive and read using Hive ${internal.writeHiveReadHive}

Backwards compatibility  tests
==============================

  parquet-1.2.5-cdh4.6.0
  ----------------------

    Can read simple MR data using MR     ${mRReadMR("parquet-1.2.5-cdh4.6.0")}
    Can read simple Hive data using MR   ${hiveReadMR("parquet-1.2.5-cdh4.6.0")}
    Can read simple MR data using hive   ${mRReadHive("parquet-1.2.5-cdh4.6.0")}
    Can read simple Hive data using hive ${hiveReadHive("parquet-1.2.5-cdh4.6.0")}

  parquet-1.2.5-cdh4.6.0-p337
  ---------------------------

    Can read simple MR data using MR     ${mRReadMR("parquet-1.2.5-cdh4.6.0-p337")}
    Can read simple Hive data using MR   ${hiveReadMR("parquet-1.2.5-cdh4.6.0-p337")}
    Can read simple MR data using hive   ${mRReadHive("parquet-1.2.5-cdh4.6.0-p337")}
    Can read simple Hive data using hive ${hiveReadHive("parquet-1.2.5-cdh4.6.0-p337")}

  parquet-1.2.5-cdh4.6.0-p485
  ---------------------------

    Can read simple MR data using MR     ${mRReadMR("parquet-1.2.5-cdh4.6.0-p485")}
    Can read simple Hive data using MR   ${hiveReadMR("parquet-1.2.5-cdh4.6.0-p485")}
    Can read simple MR data using hive   ${mRReadHive("parquet-1.2.5-cdh4.6.0-p485")}
    Can read simple Hive data using hive ${hiveReadHive("parquet-1.2.5-cdh4.6.0-p485")}


"""

  def writeMR(db: String, dst: String) = {
    val job  = withArgs(Map("db" -> db, "dst" -> dst))(new simple.MRWriteJob(_))
    val fact = hiveWarehouse </> s"$db.db" </> dst </> "*.parquet" ==> records(ParquetThermometerRecordReader[Simple], simple.Compatibility.data)

    job.withFacts(fact)
  }

  def readMR(db: String, src: String, dst: String) = {
    val job  = withArgs(Map("db" -> db, "src" -> src, "dst" -> dst))(new simple.MRReadJob(_))
    val fact = dst </> "part-*" ==> lines(simple.Compatibility.dataTsv)

    job.withFacts(fact)
  }

  def writeHive(db: String, tmpTable: String, dst: String) = {
    val job  = withArgs(Map("db" -> db, "tmpTable" -> tmpTable, "dst" -> dst))(new simple.HiveWriteJob(_))
    val fact = hiveWarehouse </> s"$db.db" </> dst </> "*" ==> records(ParquetThermometerRecordReader[Simple], simple.Compatibility.data)

    job.withFacts(fact)
  }

  def readHive(db: String, src: String, dst: String) = {
    val job  = withArgs(Map("db" -> db, "src" -> src, "dst" -> dst))(new simple.HiveReadJob(_))
    val fact = hiveWarehouse </> s"$db.db" </> dst </> "*" ==> records(ParquetThermometerRecordReader[Simple], simple.Compatibility.data)

    job.withFacts(fact)
  }

  def createExternalTable(db: String, src: String, path: String) = {
    val job = withArgs(Map("db" -> db, "src" -> src, "path" -> path))(new simple.HiveSchemaJob(_))
    job.runsOk
  }

  object internal {
    def readMRWriteMR = {
      val db  = "test"
      val src = "mrsrc"
      val dst = "mrdst"

      withDependency(writeMR(db, src))(readMR(db, src, dst))
    }

    def writeMRReadHive = {
      val db  = "test"
      val src = "mrsrc"
      val dst = "hivedst"

      withDependency(writeMR(db, src))(readHive(db, src, dst))
    }

    def writeHiveReadMR = {
      val db       = "test"
      val tmpTable = "staging"
      val src      = "hivesrc"
      val dst      = "mrdst"

      withDependency(writeHive(db, tmpTable, src))(readMR(db, src, dst))
    }

    def writeHiveReadHive = {
      val db       = "test"
      val tmpTable = "staging"
      val src      = "hivesrc"
      val dst      = "hivedst"

      withDependency(writeHive(db, tmpTable, src))(readHive(db, src, dst))
    }
  }

  def mRReadMR(parquetDir: String) =
    withEnvironment(path(getClass.getResource(s"/$parquetDir/simple/").toString)) {
      val db   = "test"
      val src  = "mrsrc"
      val path = s"$dir/user/ebenezer"
      val dst  = "mrdst"

      withDependency(createExternalTable(db, src, path))(readMR(db, src, dst))
    }

  def hiveReadMR(parquetDir: String) =
    withEnvironment(path(getClass.getResource(s"/$parquetDir/simple/").toString)) {
      val db   = "test"
      val src  = "hivesrc"
      val path = s"$dir/user/hive"
      val dst  = "mrdst"

      withDependency(createExternalTable(db, src, path))(readMR(db, src, dst))
    }

  def mRReadHive(parquetDir: String) =
    withEnvironment(path(getClass.getResource(s"/$parquetDir/simple/").toString)) {
      val db   = "test"
      val src  = "mrsrc"
      val path = s"$dir/user/ebenezer"
      val dst  = "hivedst"

      withDependency(createExternalTable(db, src, path))(readHive(db, src, dst))
    }

  def hiveReadHive(parquetDir: String) =
    withEnvironment(path(getClass.getResource(s"/$parquetDir/simple/").toString)) {
      val db   = "test"
      val src  = "hivesrc"
      val path = s"$dir/user/hive"
      val dst  = "hivedst"

      withDependency(createExternalTable(db, src, path))(readHive(db, src, dst))
    }
}
