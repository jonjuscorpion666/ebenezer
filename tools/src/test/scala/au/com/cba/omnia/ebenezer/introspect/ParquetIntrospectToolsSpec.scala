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
package introspect

import cascading.flow.FlowDef
import cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scalding.TDsl._
import com.twitter.scrooge._

import au.com.cba.omnia.ebenezer.test._, ThriftArbitraries._, JavaArbitraries._
import au.com.cba.omnia.ebenezer.scrooge._
import au.com.cba.omnia.thermometer.core._, Thermometer._
import au.com.cba.omnia.thermometer.tools._
import au.com.cba.omnia.thermometer.context._
import au.com.cba.omnia.thermometer.fact._, PathFactoids._

import java.util.UUID
import java.nio.ByteBuffer

import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.transport.TIOStreamTransport
import org.scalacheck._

import parquet.thrift.ThriftSchemaConverter
import parquet.format.Util
import parquet.hadoop.ParquetReader

object ParquetIntrospectToolsSpec extends ThermometerSpec with HadoopSupport { def is = s2"""

Introspect usage
================

  Read some parquet as records              $usage

Introspect on all types
=======================

  Read structs                              ${check("structs", fromCustomer)}
  Read booleans                             ${check("boolean", fromBoolish)}
  Read doubles                              ${check("double", fromDoublish)}
  Read bytes                                ${check("byte", fromBytish)}
  Read shorts                               ${check("short", fromShortish)}
  Read ints                                 ${check("int", fromIntish)}
  Read longs                                ${check("long", fromLongish)}
  Read strings                              ${check("string", fromStringish)}
  Read nested                               ${check("nested", fromNestedish)}
  Read lists                                ${check("list", fromListish)}
  Read maps                                 ${check("map", fromMapish)}
  Read emums                                $pending //{check("enum", fromEnumish)}

"""
  val data = List(
    Customer("CUSTOMER-1", "Fred", "Bedrock", 40),
    Customer("CUSTOMER-2", "Wilma", "Bedrock", 40),
    Customer("CUSTOMER-3", "Barney", "Bedrock", 39),
    Customer("CUSTOMER-4", "BamBam", "Bedrock", 2)
  )

  def usage =
    withData(name, data)(context => {
      /* Gather all the parquet files and read each of them as "Records" of fields / value pairs */
      val result = context.glob(name </> "*.parquet").flatMap(
        ParquetIntrospectTools.listFromPath(jobConf, _))

      result.toSet must_== data.map(fromCustomer).toSet
    })

  def fromBoolish: Boolish => Record = d => Record(List(
    Field("value", BooleanValue(d.value))
  ))

  def fromDoublish: Doublish => Record = d => Record(List(
    Field("value", DoubleValue(d.value))
  ))

  def fromBytish: Bytish => Record = d => Record(List(
    Field("value", IntValue(d.value))
  ))

  def fromShortish: Shortish => Record = d => Record(List(
    Field("value", IntValue(d.value))
  ))

  def fromIntish: Intish => Record = d => Record(List(
    Field("value", IntValue(d.value))
  ))

  def fromLongish: Longish => Record = d => Record(List(
    Field("value", LongValue(d.value))
  ))

  def fromStringish: Stringish => Record = d => Record(List(
    Field("value", StringValue(d.value))
  ))

  def fromNestedish: Nestedish => Record = d => Record(List(
    Field("value", RecordValue(
      Record(List(
        Field("value", StringValue(d.value.value))
      ))
    ))
  ))

  def fromListish: Listish => Record = d => Record(List(
    Field("values", ListValue(d.values.toList.map(StringValue)))
  ))

  def fromMapish: Mapish => Record = d => Record(List(
    Field("values", MapValue(Map(d.values.toList.map({
      case (k, v) => StringValue(k) -> StringValue(v)
    }):_*)))
  ))

  // Enum support is broken somewhere See https://github.com/CommBank/ebenezer/issues/49
  def fromEnumish: Enumish => Record = d => Record(List(
     Field("value", EnumValue(d.value.name.toUpperCase))
  ))

  def fromCustomer: Customer => Record = customer => Record(List(
    Field("id", StringValue(customer.id)),
    Field("name", StringValue(customer.name)),
    Field("address", StringValue(customer.address)),
    Field("age", IntValue(customer.age))
  ))

  def withData[A <: ThriftStruct](name: String, data: List[A])(check: Context => Unit)(implicit M: Manifest[A]) =
    ThermometerSource(data)
      .write(ParquetScroogeSource[A](name))
      .withExpectations(check)


  def check[A <: ThriftStruct](name: String, converter: A => Record)(implicit A: Arbitrary[A], M: Manifest[A]) =
    propNoShrink { (data: List[A], uuid: UUID) => isolate {
      withData(name, data)(context => {
        val result = context.glob(name </> "*.parquet").flatMap(
          ParquetIntrospectTools.listFromPath(jobConf, _))
        result.toSet must_== data.map(converter).toSet
      })
    } }.set(minTestsOk = 10)
}
