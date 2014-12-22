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

import org.apache.hadoop.mapred.{JobConf, RecordReader, OutputCollector}
import org.apache.hadoop.fs.{FileSystem, Path}

import cascading.scheme.Scheme
import cascading.tap.{Tap, SinkMode}

import cascading.tap.hive.{HiveTap, HivePartitionTap, HiveTableDescriptor}

import com.twitter.scalding._

import com.twitter.scrooge.ThriftStruct

/**
  * A scalding sink to write out Scrooge Thrift structs to a partitioned hive table using parquet as
  * underlying storage format.
  *
  * Unfortunately read does not work since the ParquetInputSplit is an instance of
  * mapreduce.FileSplit and cascading will ignore any partitioned input splits that aren't part of
  * mapred.FileSplit.
  * Instead use [[PartitionHiveParquetScroogeSource]] for read.
  *
  * @param partitionColumns a list of the partition columns formatted as `[(name, type)]`.
  * @param append iff true will add new files to an existing partition instead of overwritting it.
  */
case class PartitionHiveParquetScroogeSink[A, T <: ThriftStruct](
  database: String, table: String, partitionColumns: List[(String, String)],
  location: Option[String] = None, append: Boolean = false
) (implicit m: Manifest[T], valueSet: TupleSetter[T], partitionSet: TupleSetter[A])
    extends Source
    with TypedSink[(A, T)]
    with java.io.Serializable {

  assert(
    partitionSet.arity == partitionColumns.length,
    "The size of the partition type needs to match the number of specified partion columns"
  )
  val hdfsScheme =
    HadoopSchemeInstance(Util.parquetScheme[T])

  hdfsScheme.setSinkFields(Dsl.strFields(List("0")))

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = mode match {
    case hdfsMode @ Hdfs(_, jobConf) => readOrWrite match {
      case Read  =>
        sys.error(s"HDFS read mode is currently not supported for ${toString}. Use PartitionHiveParquetScroogeSource instead.")
      case Write => {
        if (append) {
          // Include a timestamp as part of the file name so that subsequent jobs don't override the
          // existing files.
          jobConf.set("cascading.tapcollector.partname", s"%s%spart-${System.currentTimeMillis}-%05d-%05d")
        }

        val path = location.map { l  =>
          val fs = FileSystem.get(jobConf)
          val p = fs.makeQualified(new Path(l))
          p
        }

        val tableDescriptor =
          Util.createHiveTableDescriptor[T](database, table, partitionColumns, ParquetFormat, path)

        val tap = new HivePartitionTap(
          new HiveTap(tableDescriptor, hdfsScheme, SinkMode.REPLACE, true), SinkMode.UPDATE
        )

        tap.asInstanceOf[Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]]
      }
    }
    case Local(_) => sys.error("Local mode is currently not supported for ${toString}")
    case x        => sys.error(s"$x mode is currently not supported for ${toString}")
  }

  override def sinkFields =
    Dsl.strFields((0 until valueSet.arity).map(_.toString) ++ partitionColumns.map(_._1))

  /*
   Create a setter which is the union of value and partition, it is _not_ safe to pull this out as a
   generic converter, because if anyone forgets to explicitly type annotate the A infers to Any and
   you get default coverters (yes, scala libraries, particularly scalding do this, it is not ok, but
   we must deal with it), so we hide it inside the Source so it can't be messed up. See also
   converter.
   */
  override def setter[U <: (A, T)] = Util.partitionSetter[T, A, U](valueSet, partitionSet)

  override def toString: String =
    s"PartitionHiveParquetScroogeSink[${m.runtimeClass}]($database, $table, $partitionColumns)"
}

/** Constructors for PartitionHiveParquetScroogeSink. */
object PartitionHiveParquetScroogeSink {
  /**
    * A scalding sink to write out Scrooge Thrift structs to a partitioned hive table using parquet as
    * underlying storage format.
    *
    * Unfortunately read does not work since the ParquetInputSplit is an instance of
    * mapreduce.FileSplit and cascading will ignore any partitioned input splits that aren't part of
    * mapred.FileSplit.
    * Instead use [[PartitionHiveParquetScroogeSource]] for read.
    *
    * @param partitionColumns a list of the partition columns formatted as `[(name, type.)]`.
    */
  def apply[A, T <: ThriftStruct](
    database: String, table: String, partitionColumns: List[(String, String)], location: String
  ) (implicit m: Manifest[T], valueSet: TupleSetter[T], partitionSet: TupleSetter[A]) = {
    new PartitionHiveParquetScroogeSink(database, table, partitionColumns, Some(location))(m, valueSet, partitionSet)
  }

  /**
    * A scalding sink to write out Scrooge Thrift structs to a partitioned hive table using parquet as
    * underlying storage format.
    *
    * Unfortunately read does not work since the ParquetInputSplit is an instance of
    * mapreduce.FileSplit and cascading will ignore any partitioned input splits that aren't part of
    * mapred.FileSplit.
    * Instead use [[PartitionHiveParquetScroogeSource]] for read.
    *
    * @param partitionColumns a list of the partition columns formatted as `[(name, type.)]`.
    * @param append iff true will add new files to an existing partition instead of overwritting it.
    */
  def apply[A, T <: ThriftStruct](
    database: String, table: String, partitionColumns: List[(String, String)],
    location: String, append: Boolean
  ) (implicit m: Manifest[T], valueSet: TupleSetter[T], partitionSet: TupleSetter[A]) = {
    new PartitionHiveParquetScroogeSink(database, table, partitionColumns, Some(location), append)(m, valueSet, partitionSet)
  }
}

/**
  * Custom tap for reading partitioned hive parquet tables, see [[PartitionHiveParquetScroogeSink]]
  * for why this is needed. This will add globs for all the underlying partitions to the path of the
  * hive table.
  */
class PartitionHiveParquetReadTap(
  tableDescriptor: HiveTableDescriptor, hdfsScheme: Scheme[_, _, _, _, _]
) extends HiveTap(tableDescriptor, hdfsScheme, SinkMode.KEEP, true){
  override def setStringPath(path: String): Unit = {
    super.setStringPath(s"$path/${tableDescriptor.getPartitionKeys.map(_ => "*").mkString("/")}")
  }

  /**
    * Overrides the identifier and remove the globed out partitions so that it matches the one used
    * for reading partitioned data and they are, therefore, the same for scheduling cascades.
    */
  override def getIdentifier(): String =
    getPath.toString.take(getPath.toString.length - 2 * tableDescriptor.getPartitionKeys.length)

  /**
    * Overrides the identifier and remove the globed out partitions so that it matches the one used
    * for reading partitioned data and they are, therefore, the same for scheduling cascades.
    */
  override def getFullIdentifier(conf: JobConf): String = {
    val id = super.getFullIdentifier(conf)
    id.take(id.length - 2 * tableDescriptor.getPartitionKeys.length)
  }
}
/**
  * A scalding source to read Scrooge Thrift structs from a partitioned hive table using parquet as
  * underlying storage format. It will ignore the partition columns and only read the thrift struct
  * from the parquet file.
  * Use [[PartitionHiveParquetScroogeSink]] for write.
  */
case class PartitionHiveParquetScroogeSource[T <: ThriftStruct](
  database: String, table: String, partitionColumns: List[(String, String)]
) (implicit m : Manifest[T], conv: TupleConverter[T])
    extends Source
    with Mappable[T]
    with java.io.Serializable {
  val tableDescriptor = Util.createHiveTableDescriptor[T](database, table, partitionColumns, ParquetFormat)
  val hdfsScheme =
    HadoopSchemeInstance(Util.parquetScheme[T])

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = mode match {
    case Local(_)              => sys.error("Local mode is currently not supported for ${toString}")
    case hdfsMode @ Hdfs(_, jobConf) => readOrWrite match {
      case Read  => {
        val tap = new PartitionHiveParquetReadTap(tableDescriptor, hdfsScheme)
        tap.asInstanceOf[Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]]
      }
      case Write =>
        sys.error(s"HDFS write mode is currently not supported for ${toString}. Use PartitionHiveParquetScroogeSink instead.")
    }
    case x       => sys.error(s"$x mode is currently not supported for ${toString}")
  }

  override def converter[U >: T] =
    TupleConverter.asSuperConverter[T, U](conv)

  override def toString: String =
    s"PartitionHiveParquetScroogeSource[${m.runtimeClass}]($database, $table, $partitionColumns)"
}
