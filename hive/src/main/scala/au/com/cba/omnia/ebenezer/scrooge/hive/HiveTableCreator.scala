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

import scala.util.Success
import scala.util.{Failure, Try}

import cascading.tap.hive.HiveTableDescriptor
import com.twitter.scrooge.ThriftStruct
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.{HiveMetaHookLoader, HiveMetaStoreClient, IMetaStoreClient, RetryingMetaStoreClient}
import org.apache.hadoop.hive.metastore.api.{Database, NoSuchObjectException}
import org.apache.hadoop.hive.metastore.api.Table

/** Creates hive tables with specific storage formating [[ParquetFormat]] or [[TextFormat]]  */
object HiveTableCreator {

  /**
    * Creates hive table with hive storage format 
    * 
    * @param database name of the database, will be created if not found
    * @param table name of table to create
    * @param partitionColumns list of partition columns
    * @param format storage format of the hive table specify [[ParquetFormat]] for parquet or [[TextFormat]] for text
    * @return Try instace indicating success or failure of the operation
    */
  def create[T <: ThriftStruct]
    (database: String, table: String, partitionColumns: List[(String, String)], format: HiveStorageFormat, location: Option[Path] = None)
    (implicit m: Manifest[T]): Try[Unit] = {
   
    val tableDescriptor = Util.createHiveTableDescriptor[T](database, table, partitionColumns, format, location)

    create(tableDescriptor, location)
  }

  /** Creates hive table with parquet storage format */
  def createParquet[T <: ThriftStruct]
    (database: String, table: String, partitionColumns: List[(String, String)], location: Option[Path] = None)
    (implicit m: Manifest[T]): Try[Unit] = create(database, table, partitionColumns, ParquetFormat, location)

  /** Creates hive table with text storage format */
  def createText[T <: ThriftStruct]
    (database: String, table: String, partitionColumns: List[(String, String)], location: Option[Path] = None)
    (implicit m: Manifest[T]): Try[Unit] = create(database, table, partitionColumns, TextFormat, location)

  /** Creates hive table based on table descriptor */
  def create[T <: ThriftStruct](tableDescriptor: HiveTableDescriptor, location: Option[Path] = None): Try[Unit] = {
    val path = location.map(_.toString()).getOrElse(null)
    for {
      metaStoreClient <- Try(createMetaStoreClient())
      _               <- Try(metaStoreClient.getDatabase(tableDescriptor.getDatabaseName()))
                           .recoverWith {
                             case ex: NoSuchObjectException => Try(createDatabase(tableDescriptor, path, metaStoreClient))
                             case e                         => closeResourceWithFailure(metaStoreClient, e)
                         }
      _               <- Try(createTableAndCloseMetaStoreClient(metaStoreClient, tableDescriptor))
                           .recoverWith {
                             case e => closeResourceWithFailure(metaStoreClient, e)
                           }
    } yield ()
  }

  private def createTableAndCloseMetaStoreClient(metaStoreClient: IMetaStoreClient, tableDescriptor: HiveTableDescriptor) = {
    metaStoreClient.createTable(tableDescriptor.toHiveTable())
    metaStoreClient.close()
  }

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
