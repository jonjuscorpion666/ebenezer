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

package au.com.cba.omnia.ebenezer.test

import org.specs2.Specification
import org.specs2.matcher.Matcher
import org.specs2.execute.{Result => SpecResult}

import au.com.cba.omnia.omnitool.{Result, Ok, Error}

import au.com.cba.omnia.thermometer.hive.HiveSupport

import au.com.cba.omnia.ebenezer.scrooge.hive.Hive

/** Provides matchers for `Hive`. */
trait HiveMatchers { self: Specification with HiveSupport =>
  /** Checks that the result of running the hive action has the expected Result. */
  def beResult[A](expected: Result[A]): Matcher[Hive[A]] =
    (h: Hive[A]) => h.run(hiveConf) must_== expected

  /** Checks that the result of running the hive action has the expected Result. */
  def beResultLike[A](expected: Result[A] => SpecResult): Matcher[Hive[A]] =
    (h: Hive[A]) => expected(h.run(hiveConf))

  /** Checks that the result of running the hive action has the expected value. */
  def beValue[A](expected: A): Matcher[Hive[A]] =
    beResult(Result.ok(expected))
}
