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

import scala.util.{Failure, Try, Success}

import cascading.tap.hive.HiveTableDescriptor
import com.twitter.scrooge.ThriftStruct
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.{HiveMetaHookLoader, HiveMetaStoreClient, IMetaStoreClient, RetryingMetaStoreClient}
import org.apache.hadoop.hive.metastore.api.{Database, NoSuchObjectException}
import org.apache.hadoop.hive.metastore.api.Table

/**  Hive operations */
object HiveOps {

  /**
    * Creates hive table with hive storage format.
    *
    * WARNING: This method is not thread safe.
    *
    * @param database Name of the database. Will be created if not found.
    * @param table Name of table to create.
    * @param partitionColumns A list of the partition columns formatted as `[(name, type.)]`.
    *                         If empty unpartitioned table will be created.
    * @param location Optional location for the hive table. Not set by default.
    * @param format Storage format of the hive table. Defaults to [[ParquetFormat]].
    */
  def createTable[T <: ThriftStruct](
    database: String, table: String, partitionColumns: List[(String, String)],
    location: Option[Path] = None, format: HiveStorageFormat = ParquetFormat
  )(implicit m: Manifest[T]): Try[Unit] = {
    val tableDescriptor = Util.createHiveTableDescriptor[T](database, table, partitionColumns, format, location)
    createImpl(tableDescriptor, location)
  }

  private def createImpl[T <: ThriftStruct](
    tableDescriptor: HiveTableDescriptor, location: Option[Path]
  ): Try[Unit] = withMetaStoreClient(client => for {
    _ <- Try(client.getDatabase(tableDescriptor.getDatabaseName)).recoverWith {
      case e: NoSuchObjectException => createDatabase(tableDescriptor, location, client)
    }
    _ <- Try(client.createTable(tableDescriptor.toHiveTable))
  } yield () )

  private def createDatabase(tableDescriptor: HiveTableDescriptor, location: Option[Path], metaStoreClient: IMetaStoreClient) = Try {
    val db   = new Database(
      tableDescriptor.getDatabaseName(),
      "created by Ebenezer",
      location.map(_.toString).getOrElse(null),
      null
    )
    metaStoreClient.createDatabase(db)
  }

  protected[hive] def withMetaStoreClient[A](action: IMetaStoreClient => Try[A]): Try[A] = {
    val createClient = Try {
      val hiveConf = new HiveConf()
      RetryingMetaStoreClient.getProxy(
        hiveConf,
        new HiveMetaHookLoader() {
          override def getHook(tbl: Table) = null
        },
        classOf[HiveMetaStoreClient].getName()
      )
    }
    createClient.flatMap(client => action(client).transform(
      ok => { client.close; Success(ok) },
      ex => { client.close; Failure(ex) }
    ))
  }
}
