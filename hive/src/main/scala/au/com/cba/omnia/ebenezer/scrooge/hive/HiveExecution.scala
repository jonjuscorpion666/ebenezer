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

package au.com.cba.omnia.ebenezer.scrooge
package hive

import collection.JavaConverters._

import com.twitter.scalding.{Execution, Source, Write, Mode, Args}

import org.apache.hadoop.conf.Configuration

import cascading.tap.Tap

import cascading.flow.hive.HiveFlow

import org.apache.hadoop.hive.conf.HiveConf.ConfVars

/** Methods to run Hive queries inside the Execution monad. */
@deprecated("Use the query/queries method in Hive instead", "0.12")
object HiveExecution {
  /** Runs the specified hive queries inside the Execution monad. */
  @deprecated("Use the query/queries method in Hive instead", "0.12")
  def query(name: String, queries: String*): Execution[Unit] =
    rawQuery(name, None, Map.empty, queries)

  /**
    * Runs the specified hive queries inside the Execution monad.
    *
    * The specified output is used to create the target table before the job starts.
    */
  @deprecated("Use the query/queries method in Hive instead", "0.12")
  def query(name: String, output: Source, queries: String*): Execution[Unit] =
    rawQuery(name, Some(output), Map.empty, queries)

  /** Runs the specified hive queries inside the Execution monad. */
  @deprecated("Use the query/queries method in Hive instead", "0.12")
  def query(name: String, hiveSettings: Map[ConfVars, String], queries: String*): Execution[Unit] =
    rawQuery(name, None, hiveSettings, queries)

  /**
    * Runs the specified hive queries inside the Execution monad.
    *
    * The specified output is used to create the target table before the job starts.
    */
  @deprecated("Use the query/queries method in Hive instead", "0.12")
  def query(name: String, output: Source, hiveSettings: Map[ConfVars, String], queries: String*): Execution[Unit] =
    rawQuery(name, Some(output), hiveSettings, queries)

  /** Private implementation of running a hive query inside the execution monad. */
  private def rawQuery(
    name: String, dst: Option[Source],
    hiveSettings: Map[ConfVars, String], queries: Seq[String]
  ): Execution[Unit] = Execution.getMode.flatMap { mode =>
    val inputTaps: List[Tap[_, _, _]] =
      List(new NullTap("Hive IN"))

    val flow = new HiveFlow(
      name, queries.toArray, inputTaps.asJava,
      dst.fold[Tap[_, _, _]](new NullTap("Hive OUT"))(_.createTap(Write)(mode)),
      hiveSettings.map { case (key, value) => key.varname -> value }.asJava
    )

    flow.setFlowSkipStrategy(DontSkipStrategy)

    Execution.fromFuture(_ => Execution.run(flow)).map(_ => ())
  }

}
