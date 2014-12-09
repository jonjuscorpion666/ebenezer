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

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.metastore.api.FieldSchema

import org.specs2.Specification
import org.specs2.matcher.ThrownExpectations

import org.apache.hadoop.hive.metastore.api.StorageDescriptor

class UtilSpec extends Specification with ThrownExpectations { def is = s2"""

UtilSpec
========

  create a valid  table descriptor for primitive types $primitives
  create a valid table descriptor for list $list
  create a valid table descriptor for map $map
  create a valid table descriptor for nested maps and lists $nested

"""

  val com = "created by Cascading"

  def primitives =  {
    val expected = List(
      new FieldSchema("boolean", "boolean", com), new FieldSchema("byte", "tinyint", com),
      new FieldSchema("short", "smallint", com), new FieldSchema("integer", "int", com),
      new FieldSchema("long", "bigint", com), new FieldSchema("double", "double", com),
      new FieldSchema("string", "string", com)
    )

    val td = Util.createHiveTableDescriptor[Primitives]("db", "test", List.empty, ParquetFormat)
    val actual = td.toHiveTable.getSd.getCols.asScala.toList

    actual must_== expected
  }

  def list =  {
    val expected = List(
      new FieldSchema("short", "smallint", com), new FieldSchema("list", "array<int>", com)
    )

    val td = Util.createHiveTableDescriptor[Listish]("db", "test", List.empty, ParquetFormat)
    val sd = td.toHiveTable.getSd
    val actual = sd.getCols.asScala.toList

    verifyInputOutputFormatForParquet(sd)
    actual must_== expected
  }

  def map =  {
    val expected = List(
      new FieldSchema("short", "smallint", com), new FieldSchema("map", "map<int,string>", com)
    )

    val td = Util.createHiveTableDescriptor[Mapish]("db", "test", List.empty, ParquetFormat)
    val actual = td.toHiveTable.getSd.getCols.asScala.toList

    actual must_== expected
  }

  def nested =  {
    val expected = List(
      new FieldSchema("nested", "map<int,map<string,array<int>>>", com)
    )

    val td = Util.createHiveTableDescriptor[Nested]("db", "test", List.empty, ParquetFormat)
    val sd = td.toHiveTable.getSd

    val actual = sd.getCols.asScala.toList

    verifyInputOutputFormatForParquet(sd)
    actual must_== expected
  }

  def verifyInputOutputFormatForParquet(sd: StorageDescriptor) {
    sd.getInputFormat() must_== "parquet.hive.DeprecatedParquetInputFormat"
    sd.getOutputFormat() must_== "parquet.hive.DeprecatedParquetOutputFormat"
  }
}
