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

import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeScheme
import cascading.scheme.Scheme
import cascading.scheme.hadoop.TextDelimited
import cascading.scheme.hadoop.TextLine

import scala.collection.{Map => CMap}

import org.apache.thrift.protocol.TType

import cascading.tuple.{Fields, Tuple}

import org.apache.hadoop.fs.Path

import com.twitter.scalding.{Dsl, TupleConverter, TupleSetter}

import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec, ThriftStructField}

import cascading.tap.hive.HiveTableDescriptor

import au.com.cba.omnia.ebenezer.reflect.Reflect

/**
  * Utility methods used by the Hive sources in this packages to avoid serialisation issues and code
  * duplication.
  */
object Util {
  /*
   DO NOT USE intFields, scalding / cascading Fields.merge is broken and gets called in bowels of
   TemplateTap. See scalding/#803.
   */
  def toFields(start: Int, end: Int): Fields =
    Dsl.strFields((start until end).map(_.toString))

  /** A tuple converter that splits a cascading tuple into a pair of types.*/
  def converter[A, T, U >: (A, T)]
    (valueConverter: TupleConverter[T], partitionConverter: TupleConverter[A]) =
    TupleConverter.asSuperConverter[(A, T), U](new TupleConverter[(A, T)] {
      import cascading.tuple.TupleEntry

      def arity = valueConverter.arity + partitionConverter.arity

      def apply(te : TupleEntry) : (A, T) = {
        val value = new TupleEntry(toFields(0, valueConverter.arity))
        val partition = new TupleEntry(toFields(0, partitionConverter.arity))
          (0 until valueConverter.arity).foreach(idx => value.setObject(idx, te.getObject(idx)))
          (0 until partitionConverter.arity).foreach(idx =>
            partition.setObject(idx, te.getObject(idx + valueConverter.arity)))
        
        (partitionConverter(partition), valueConverter(value))
      }
    })

  /** A tuple setter for a pair of types which are flattened into a cascading tuple.*/
  def partitionSetter[T, A, U <: (A, T)](valueSet: TupleSetter[T], partitionSet: TupleSetter[A])
    : TupleSetter[U] =
    TupleSetter.asSubSetter[(A, T), U](new TupleSetter[(A, T)] {

      def arity = valueSet.arity + partitionSet.arity

      def apply(arg: (A, T)) = {
        val (a, t) = arg
        val partition = partitionSet(a)
        val value = valueSet(t)
        val output = Tuple.size(partition.size + value.size)
          (0 until value.size).foreach(idx => output.set(idx, value.getObject(idx)))
          (0 until partition.size).foreach(idx =>
            output.set(idx + value.size, partition.getObject(idx))
          )
        output
      }
    })

  /** Creates a hive parquet table descriptor based on a thrift struct.*/
  def createHiveTableDescriptor[T <: ThriftStruct]
    (database: String, table: String, partitionColumns: List[(String, String)], format: HiveStorageFormat, location: Option[Path] = None)
    (implicit m: Manifest[T])= {
    val thrift: Class[T]     = m.runtimeClass.asInstanceOf[Class[T]]
    val codec                = Reflect.companionOf(thrift).asInstanceOf[ThriftStructCodec[_ <: ThriftStruct]]
    val metadata             = codec.metaData
    val partitionColumnNames = partitionColumns.map(_._1)
    val partitionColumnTypes = partitionColumns.map(_._2)
    val structColumns        = metadata.fields.sortBy(_.id)
    val structColumnNames    = structColumns.map(_.name)
    val structColumnTypes    = structColumns.map(t => Util.mapType(t))
    val columns              = (structColumnNames ++ partitionColumnNames).toArray
    val types                = (structColumnTypes ++ partitionColumnTypes).toArray

    assert(
      partitionColumnNames.intersect(metadata.fields.map(_.name)).isEmpty,
      "Partition columns must be different from the fields in the thrift struct"
    )

    format match {
      case TextFormat    =>
        new HiveTableDescriptor(
          database, table, columns, types,
          partitionColumns.map(_._1).toArray,
          HiveTableDescriptor.HIVE_DEFAULT_DELIMITER,
          HiveTableDescriptor.HIVE_DEFAULT_SERIALIZATION_LIB_NAME,
          location.getOrElse(null)
        )
      case ParquetFormat =>
        new ParquetTableDescriptor(
          database, table, columns, types,
          partitionColumns.map(_._1).toArray, location.getOrElse(null)
        )
    }
  }

  def createSchemaBasedOnFormat[T <: ThriftStruct]
    (format: HiveStorageFormat)
    (implicit m: Manifest[T]): Scheme[_, _, _, _, _] = format match {
    case TextFormat    => throw new UnsupportedOperationException("not yet supported")
    case ParquetFormat => new ParquetScroogeScheme[T].asInstanceOf[Scheme[_, _, _, _, _]]
  }

  /** Maps Thrift types to Hive types.*/
  def mapType(field: ThriftStructField[_]): String = {
    field.`type` match {
      case TType.BOOL   => "boolean"
      case TType.BYTE   => "tinyint"
      case TType.I16    => "smallint"
      case TType.I32    => "int"
      case TType.I64    => "bigint"
      case TType.DOUBLE => "double"
      case TType.STRING => "string"
        
      // 1 type param
      case TType.LIST   => {
        val elementType = toThriftType(argsOf(field).head)
        s"array<$elementType>"
      }
      case TType.SET    => throw new Exception("SET is not a supported Hive type")
      case TType.ENUM   => throw new Exception("ENUM is not a supported Hive type")
        
      // 2 type params
      case TType.MAP    => {
        val args      = argsOf(field)
        val keyType   = toThriftType(args(0))
        val valueType = toThriftType(args(1))
        s"map<$keyType,$valueType>"
      }
        
      // n type params
      case TType.STRUCT => throw new Exception("STRUCT is not a supported Hive type")
        
      // terminals
      case TType.VOID   => throw new Exception("VOID is not a supported Hive type")
      case TType.STOP   => throw new Exception("STOP is not a supported Hive type")
    }
  }

  /** Gets the manifests of the type arguments for complex thrift types such as Map. */
  def argsOf(field: ThriftStructField[_]): List[Manifest[_]] = {
    field.manifest.get.typeArguments
  }

  /** Maps manifest information to hive types. */
  def toThriftType(mani: Manifest[_]): String = {
    if (manifest[Boolean] == mani)
      "boolean"
    else if (manifest[Byte] == mani)
      "tinyint"
    else if (manifest[Double] == mani)
      "double"
    else if (manifest[Short] == mani)
      "smallint"
    else if (manifest[Int] == mani)
      "int"
    else if (manifest[Long] == mani)
      "bigint"
    else if (manifest[String] == mani)
      "string"
    else if (manifest[CMap[_, _]].runtimeClass == mani.runtimeClass) {
      val args      = mani.typeArguments
      val keyType   = toThriftType(args(0))
      val valueType = toThriftType(args(1))
      s"map<$keyType,$valueType>"
    } else if (manifest[Seq[_]].runtimeClass == mani.runtimeClass) {
      val elementType = toThriftType(mani.typeArguments.head)
      s"array<$elementType>"
    } else
      throw new Exception(s"$mani is not a supported nested type")
  }

  /** Gets the manifest for `T`. */
  def manifest[T : Manifest]: Manifest[T] = implicitly[Manifest[T]]
}
