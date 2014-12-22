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

package au.com.cba.omnia.ebenezer.scrooge.hive

import scala.util.Try

import au.com.cba.omnia.thermometer.core.{ Thermometer, ThermometerSource, ThermometerSpec }, Thermometer._
import au.com.cba.omnia.thermometer.hive.HiveSupport

import au.com.cba.omnia.ebenezer.hive.SimpleHive
import com.twitter.scalding.Write
import org.apache.hadoop.mapred.JobConf

object HiveTableCreatorSpec extends ThermometerSpec with HiveSupport { def is = s2"""
=========================

  creates hive table with TextFormat     $newTextTable
  creates hive table with ParquetFormat  $newParquetTable
"""

  def newTextTable = newTableTest(
    "testtext", "normalhive", TextFormat(), "org.apache.hadoop.mapred.TextInputFormat"
  )

  def newParquetTable = newTableTest(
    "testparquet", "normalhive", ParquetFormat, "parquet.hive.DeprecatedParquetInputFormat"
  )

  def newTableTest(newTableName: String, newDbName: String, format: HiveStorageFormat, expectedFormatClass: String) = {
    HiveOps.createTable[SimpleHive](newDbName, newTableName, List(), None, format)

    val table         = HiveOps.withMetaStoreClient(client => Try(client.getTable(newDbName, newTableName))).get
    val columns       = table.getSd.getCols
    val hiveTableName = table.getTableName

    table.getSd.getInputFormat must_== expectedFormatClass
    columns.get(0).getName     must_== "stringfield"
    newTableName               must_== hiveTableName
  }
}
