package com.inadco.cassandra.spark.jdbc

import java.util._
import com.datastax.driver.core._
import org.apache.spark.SparkConf
import com.datastax.spark.connector.cql.CassandraConnector
/**
 * DAO class to extract metadata of all the tables available in Cassandra
 * @author kmathur
 */
class CassandraMetaDataDAO (conf : SparkConf){
	val rowsList : List[Row]=init(conf)
	val SCHEMA: String ="keyspace_name"
//	val TABLE:  String ="columnfamily_name"
	val TABLE:  String ="table_name"
	val COLUMN: String ="column_name"
//	val COLUMN_DATA_TYPE: String = "validator"
	val COLUMN_DATA_TYPE: String = "type"


	def init(conf: SparkConf) : List[Row] = {
		val resultSetTables = CassandraConnector(conf).withSessionDo {
          /*
          Evaluate to use those queries
          - select keyspace_name from system.schema_keyspaces;
          - select keyspace_name,columnfamily_name from system.schema_columnfamilies where keyspace_name = 'sp'
           */
      // cassandra 2.x
      //session => session.execute("select keyspace_name, columnfamily_name, column_name,validator from system.schema_columns")

      // cassandra 3.x
			session => session.execute("select keyspace_name, table_name, column_name,type from system_schema.columns")
		}
		return resultSetTables.all()
	}
	/**
	 * Get list of all keyspaces excluding system keyspace
	 */
	def getKeySpaceList() : scala.collection.mutable.Set[String]  = {
		if(rowsList==null){
			return null
		}
		val keyspaceList = scala.collection.mutable.Set[java.lang.String]()
		
		for(x <- 0 until rowsList.size()){
			val next=rowsList.get(x);
			//take keyspace excluding: "system", "system_*", "OpsCenter"
			val ks = next.getString(SCHEMA)
			if(ks != "system" && ks.indexOf("system_") != 0 && ks != "OpsCenter"){
				keyspaceList+=next.getString(SCHEMA)
			}			
		}
		return keyspaceList
	}
	/**
	 * Get a list of all tables by a keyspace
	 */
	def getTableList(keyspace: String) : scala.collection.mutable.Set[String]  = {
		if(rowsList==null){
			return null
		}
		val tables = scala.collection.mutable.Set[java.lang.String]()
		
		for(x <- 0 until rowsList.size()){
			val next=rowsList.get(x);
			if(next.getString(SCHEMA)==keyspace){
				tables+=next.getString(TABLE)
			}
		}
		return tables
	}
	/**
	 * Get a list of all columns of a table
	 */
	def getTableColumns(schema:String,table:String) : collection.mutable.Map[String, String] = {
		if(rowsList==null){
			return null
		}
		val columns = collection.mutable.Map[String, String]()
		var x=0;
		for(x <- 0 until rowsList.size()){
			val next=rowsList.get(x);
			if(next.getString(SCHEMA)==schema && next.getString(TABLE)==table){
				columns.put(next.getString(COLUMN),next.getString(COLUMN_DATA_TYPE));
				}
			}
		return columns
	}
}
