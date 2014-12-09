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

/**
  *  Data type used to indicate Input and Output format for Hive table
  */
sealed trait HiveFormat

/**
  * By passing [[ParquetFormat]] following classes will be used to format input and output  
  * PARQUET_INPUT_FORMAT = "parquet.hive.DeprecatedParquetInputFormat"
  * PARQUET_OUTPUT_FORMAT = "parquet.hive.DeprecatedParquetOutputFormat" 
  */
case object ParquetFormat extends HiveFormat

/**
  * By passing [[TextFormat]] default Hive input / output classes will be used
  * INPUT_FORMAT_NAME = "org.apache.hadoop.mapred.TextInputFormat"
  * OUTPUT_FORMAT_NAME = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
  */
case object TextFormat    extends HiveFormat
