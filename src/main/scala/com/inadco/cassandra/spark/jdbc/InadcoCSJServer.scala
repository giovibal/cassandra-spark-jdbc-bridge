package com.inadco.cassandra.spark.jdbc

import java.io.File
import java.sql.Timestamp
import java.util.{Date, Calendar, GregorianCalendar, TimeZone}

import akka.actor._
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.sql.types._
import org.apache.spark.{Logging, SparkConf, SparkContext}


/**
 * An spark app read and register all Cassandra tables as schema RDDs in Spark SQL and starts an embedded HiveThriftServer2 to make those tables accessible via jdbc:hive2 protocol 
 * Notes:
 * - Currently only support basic/primitive data types.
 * - Cassandra table definitions are fetched every 3 seconds. A new RDD will be created under the same name of the corresponding Cassandra table is changed
 * @author hduong
 */
object InadcoCSJServer extends Logging {
	def main(args: Array[String]){
		try {
			val server = new InadcoCSJServer()
			server.init()
			server.start()
		} catch {
			case e: Exception =>
				logError("Error starting InadcoHiveThriftServer", e)
			System.exit(-1)
		}
	}

}

class InadcoCSJServer extends Logging{
	val system = ActorSystem("System")
	val hiveTables = new scala.collection.mutable.HashMap[String, StructType]()
	val appConfig = loadConfig()
	def init(){
		
	}
	def loadConfig()={
		
	  //load all the properties files
		val defaultConf = ConfigFactory.load()
		val overrideFile = new File(System.getenv("INADCO_CSJ_HOME") + "/config/csjb-default.properties")
		if(overrideFile.exists()){
			logInfo("Found override properties from: " + overrideFile.toString())
		}
		ConfigFactory.parseFile(overrideFile).withFallback(defaultConf)
	}
	
	def start(){
		logInfo("Starting InadcoCSJBServer.....")
		
		//init new spark context
		val sparkConf = new SparkConf()
//    sparkConf.setMaster("local[1]")
    sparkConf.setAppName("InadcoCSJServer")
    sparkConf.set("spark.scheduler.mode", "FAIR")
		sparkConf.set("spark.cores.max", appConfig.getString("spark.cores.max"))
		sparkConf.set("spark.cassandra.connection.host",appConfig.getString("spark.cassandra.connection.host"))
//    sparkConf.set("spark.cassandra.auth.username", appConfig.getString("spark.cassandra.auth.username"))
//		sparkConf.set("spark.cassandra.auth.password", appConfig.getString("spark.cassandra.auth.password"))
		sparkConf.set("spark.executor.memory", appConfig.getString("spark.executor.memory"))
    sparkConf.set("spark.sql.hive.thriftServer.singleSession", "true")
		
		
		sparkConf.setMaster(appConfig.getString("inadco.spark.master"))
		sparkConf.setAppName(appConfig.getString("inadco.appName"))
		val sc = new SparkContext(sparkConf)
		
		//add handler to gracefully shutdown
		Runtime.getRuntime.addShutdownHook(
			new Thread() {
	  		override def run() {
	      	logInfo("Shutting down InadcoHiveThriftServer...")
	        if(sc != null){
	        	sc.stop()
	        }
	      	logInfo("Spark context stopped.")
	  		}
      })
        
    //hive stuff
		val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    hiveContext.udf.register("toDateTime", (year:Int,month:Int,day:Int,hour:Int,timeZone:String) => {
      val tz = TimeZone.getTimeZone(timeZone)
      val m = if (month > 0) month-1 else month
      val cal = new GregorianCalendar(year, m, day, hour, 0, 0)
      cal.setTimeZone(tz)
      new Timestamp(cal.getTime.getTime)
    })
    hiveContext.udf.register("trunc_datetime", (d:Timestamp, res:String) => {
      var dateTruncated : Date = d
      val cal = new GregorianCalendar()
      cal.setTimeZone(TimeZone.getTimeZone("UTC"))
      cal.setTime(d)

      if(res.equalsIgnoreCase("YEAR")) {
        dateTruncated = DateUtils.truncate(cal, Calendar.YEAR)
      }
      else if(res.equalsIgnoreCase("MONTH")) {
        dateTruncated = DateUtils.truncate(cal, Calendar.MONTH)
      }
      else if(res.equalsIgnoreCase("DAY") || res.equalsIgnoreCase("DAY_OF_MONTH")) {
        dateTruncated = DateUtils.truncate(cal, Calendar.DAY_OF_MONTH)
      }
      else if(res.equalsIgnoreCase("WEEK")) {
        cal.setFirstDayOfWeek(Calendar.MONDAY)
        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
        cal.set(Calendar.HOUR_OF_DAY, 0)
        cal.set(Calendar.MINUTE, 0)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MILLISECOND, 0)
        dateTruncated = cal.getTime
      }
      else if(res.equalsIgnoreCase("HOUR")) {
        dateTruncated = DateUtils.truncate(d, Calendar.HOUR)
      }
      else if(res.equalsIgnoreCase("MINUTE")) {
        dateTruncated = DateUtils.truncate(d, Calendar.MINUTE)
      }
      else if(res.equalsIgnoreCase("SECOND")) {
        dateTruncated = DateUtils.truncate(d, Calendar.SECOND)
      }
      new Timestamp(dateTruncated.getTime)
    })

    HiveThriftServer2.startWithContext(hiveContext)



		//register all Cassandra tables		
//		val startDelayMs = new FiniteDuration(0, java.util.concurrent.TimeUnit.MILLISECONDS)
//		val intervalMs = new FiniteDuration(appConfig.getLong("inadco.tableList.refresh.intervalMs"), java.util.concurrent.TimeUnit.MILLISECONDS)
//
//		val cancellable = system.scheduler.schedule(startDelayMs, intervalMs)({
			registerCassandraTables(sc, sparkConf, hiveContext)
//		})

//    val tableNames : Seq[String] = hiveContext.tableNames().toSeq
//    logInfo(s"Registered tables: ${tableNames}")
//
//    val c = hiveContext.sql("select * from sp_snapshots limit 5").count()
//    logInfo(s"Count from snapshots: ${c}")

		logInfo("InadcoCSJServer started successfully")
	}
	def stop(){
		
	}
	
	def registerCassandraTables(sc: SparkContext, sparkConf: SparkConf, hiveContext: HiveContext){
    import hiveContext.implicits._
    val cassMetaDataDAO = new CassandraMetaDataDAO(sparkConf)
    val keyspaceList = cassMetaDataDAO.getKeySpaceList()
    keyspaceList.foreach { keyspace =>
      cassMetaDataDAO.getTableList(keyspace).foreach { tableName =>
        registerCassandraTable(keyspace, tableName, cassMetaDataDAO, sc, hiveContext)
      }
    }
	}
	
	def registerCassandraTable(keyspace: String, tableName: String, cassMetaDataDAO: CassandraMetaDataDAO, sc: SparkContext, hiveContext: HiveContext){
    //format full table name with keyspace_ prefix
    val hiveTableName = keyspace + "_" + tableName
    logInfo(s"Try to register hive table ${hiveTableName} ...")
    try {
      val rowSchemaDF = hiveContext.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map( "table" -> tableName, "keyspace" -> keyspace, "cluster" -> "Test Cluster" ))
        .load()

      val colums : Seq[String] = rowSchemaDF.columns.toSeq
      logInfo(s"Colums of table ${hiveTableName}: ${colums}")

      rowSchemaDF.registerTempTable(hiveTableName)

      logInfo(s"Registered table ${hiveTableName}")
		} catch {
			case e: Exception => logError("Failed to register table " + hiveTableName, e)
		}
	}



}



