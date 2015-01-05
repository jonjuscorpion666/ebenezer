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

import cascading.scheme.Scheme
import cascading.tuple.Fields

import com.twitter.scalding._
import com.twitter.scrooge._

import org.apache.thrift._

import parquet.cascading._

case class ParquetScroogeSource[T <: ThriftStruct](p : String*)(implicit m : Manifest[T], conv: TupleConverter[T], set: TupleSetter[T])
  extends FixedPathSource(p: _*)
  with TypedSink[T]
  with Mappable[T]
  with java.io.Serializable {

  override def hdfsScheme = HadoopSchemeInstance(new ParquetScroogeScheme[T].asInstanceOf[Scheme[_, _, _, _, _]])

  override def converter[U >: T] =
    TupleConverter.asSuperConverter[T, U](conv)

  override def setter[U <: T] =
    TupleSetter.asSubSetter[T, U](TupleSetter.of[T])
}
