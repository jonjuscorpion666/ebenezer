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
package hive

import au.com.cba.omnia.thermometer.core.{Thermometer, ThermometerSource, ThermometerSpec}, Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoid
import au.com.cba.omnia.thermometer.hive.HiveSupport

import au.com.cba.omnia.ebenezer.scrooge.hive._
import com.twitter.scalding.AccessMode
import com.twitter.scalding.Write

object HiveParquetSpec extends ThermometerSpec with HiveSupport { def is = s2"""
Hive Parquet properties
=========================

  can create table with ParquetFormat          $normalHive
"""

  def normalHive = {
    source
      .write(HiveParquetScroogeSource[SimpleHive]("normalhive", "test", None))
      .withFacts(
        hiveWarehouse </> "normalhive.db" </> "test" </> "*.parquet" ==> matchesFile
      )
  }

  def data = List(SimpleHive(""), SimpleHive("abc"), SimpleHive("def"))
  def source =  ThermometerSource(data)
  def matchesFile = PathFactoid((context, path) => !context.glob(path).isEmpty)
}
