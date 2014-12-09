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

import cascading.tap.hive.HiveTableDescriptor
import com.twitter.scrooge.ThriftStruct
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.{HiveMetaHookLoader, HiveMetaStoreClient, IMetaStoreClient, RetryingMetaStoreClient}
import org.apache.hadoop.hive.metastore.api.{Database, NoSuchObjectException}
import org.apache.hadoop.hive.metastore.api.Table
import scala.util.Success

import scala.util.{Failure, Try}

object HiveTableCreator {

  /** Creates hive table  */
  def create[T <: ThriftStruct]
    (database: String, table: String, partitionColumns: List[(String, String)], format: HiveFormat, location: Option[Path] = None)
    (implicit m: Manifest[T]): Try[Unit] = {
   
    val path = location.map(_.toString()).getOrElse(null)
    val tableDescriptor = Util.createHiveTableDescriptor[T](database, table, partitionColumns, format, location)

    create(tableDescriptor, path)
  }
  
  def create[T <: ThriftStruct](tableDescriptor: HiveTableDescriptor, path: String): Try[Unit] =
    for {
      metaStoreClient <- Try(createMetaStoreClient())
      _               <- Try(metaStoreClient.getDatabase(tableDescriptor.getDatabaseName()))
                           .recoverWith {
                             case ex: NoSuchObjectException => Try(createDatabase(tableDescriptor, path, metaStoreClient))
                             case e                         => closeResourceWithFailure(metaStoreClient, e)
                         }
      _               <- Try(metaStoreClient.createTable(tableDescriptor.toHiveTable()))
                           .recoverWith {
                             case e => closeResourceWithFailure(metaStoreClient, e)
                           }
    } yield ()

  private def createDatabase(tableDescriptor: HiveTableDescriptor, path: String, metaStoreClient: IMetaStoreClient) = {
    val db = new Database(
      tableDescriptor.getDatabaseName(),
      "created by Ebenezer",
      path,
      null
    )
    metaStoreClient.createDatabase(db)
  }

  private def closeResourceWithFailure(res: IMetaStoreClient, e: Throwable) = {
    res.close()
    Failure(e)
  }

  /**
    * @return new IMetaStoreClient
    * @throws MetaException in case the creation fails.
    */
  def createMetaStoreClient(): IMetaStoreClient = {
    val hiveConf = new HiveConf()

    RetryingMetaStoreClient.getProxy(
      hiveConf,
      new HiveMetaHookLoader() {
        override def getHook(tbl: Table) = null
      },
      classOf[HiveMetaStoreClient].getName()
    )
  }
}
