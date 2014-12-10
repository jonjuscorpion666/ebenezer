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

package au.com.cba.omnia.ebenezer
package scrooge
package hive

import cascading.scheme.Scheme
import cascading.tap.{Tap, SinkMode}

import cascading.tap.hive.HiveTap

import org.apache.hadoop.fs.{FileSystem, Path}

import com.twitter.scalding._

import com.twitter.scrooge.ThriftStruct

/** 
  * Source to read unpartitioned Hive tables where the data is the specified thrift struct stored
  * in Parquet format.
  */
case class HiveParquetScroogeSource[T <: ThriftStruct]
  (database: String, table: String, location: Option[String] = None)
  (implicit m : Manifest[T], conv: TupleConverter[T], set: TupleSetter[T])
  extends Source
  with TypedSink[T]
  with Mappable[T]
  with java.io.Serializable {

  lazy val hdfsScheme =
    HadoopSchemeInstance(Util.createSchemaBasedOnFormat(ParquetFormat))

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = mode match {
    case Local(_)              => sys.error("Local mode is currently not supported for ${toString}")
    case hdfsMode @ Hdfs(_, jobConf) => {
      val path = location.map { l  =>
        val fs = FileSystem.get(jobConf)
        val p = fs.makeQualified(new Path(l))
        p
      }
      val tableDescriptor =
        Util.createHiveTableDescriptor[T](database, table, List(), ParquetFormat, path)

      CastHfsTap(new HiveTap(tableDescriptor, hdfsScheme, SinkMode.REPLACE, true))
    }
    case x                     => sys.error(s"$x mode is currently not supported for ${toString}")
  }

  override def toString: String = s"HiveParquetScroogeSource[${m.runtimeClass}]($database, $table)"

  override def converter[U >: T] =
    TupleConverter.asSuperConverter[T, U](conv)

  override def setter[U <: T] =
    TupleSetter.asSubSetter[T, U](TupleSetter.of[T])
}

/** Constructors for HiveParquetScroogeSource. */
object HiveParquetScroogeSource {
  /**
    * Source to read unpartitioned Hive tables where the data is the specified thrift struct stored
    * in Parquet format.
    */
  def apply[T <: ThriftStruct : Manifest : TupleConverter : TupleSetter]
  (database: String, table: String, location: String) =
    new HiveParquetScroogeSource[T](database, table, Some(location))
}
