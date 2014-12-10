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

package au.com.cba.omnia.ebenezer.example

import com.twitter.scalding._, TDsl._
import com.twitter.scalding.typed.IterablePipe

import au.com.cba.omnia.ebenezer.scrooge.hive.HiveParquetScroogeSource

class HiveExampleStep4(args: Args) extends Job(args) {
  val data = List(
    Nested(Map(
      1 -> Map(10 -> List("a1", "b1")),
      2 -> Map(20 -> List("a2", "b2")),
      3 -> Map(30 -> List("a3", "b3"))
    )),
    Nested(Map(
      1 -> Map(10 -> List("q1", "r1")),
      2 -> Map(20 -> List("q2", "r2")),
      3 -> Map(30 -> List("q3", "r3"))
    )),
    Nested(Map(
      1 -> Map(10 -> List("x1", "z1")),
      2 -> Map(20 -> List("x2", "z2")),
      3 -> Map(30 -> List("x3", "z3"))
    ))
  )
  
  IterablePipe(data)
    .write(HiveParquetScroogeSource[Nested](args("db"), args("table")))
}
