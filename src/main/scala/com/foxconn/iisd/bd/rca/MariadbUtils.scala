package com.foxconn.iisd.bd.rca

import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.foxconn.iisd.bd.rca.XWJKernelEngine.configLoader
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class MariadbUtils {

    private var _conn: Connection = null

    def getConn(): Connection = {
        if(_conn == null || _conn.isClosed) {

            _conn = DriverManager.getConnection(
                this.getMariadbUrl(),
                this.getMariadbConnectionProperties())

            _conn.setAutoCommit(false)
        }

        return _conn
    }

    def getNewConn(): Connection = {

        val conn = DriverManager.getConnection(
            this.getMariadbUrl(),
            this.getMariadbConnectionProperties())

        conn.setAutoCommit(false)

        return conn
    }

    def closeConn(): Unit = {
        if(_conn != null && !_conn.isClosed) {
            _conn.close()
            _conn = null
        }
    }

    private def getMariadbUrl(): String = {
        return configLoader.getString("mariadb", "conn_str")
    }

    private def getMariadbUser(): String = {
        return configLoader.getString("mariadb", "username")
    }

    private def getMariadbPassword(): String = {
        return configLoader.getString("mariadb", "password")
    }

    private def getMariadbConnectionProperties(): Properties ={
        val _mariadbConnectionProperties = new Properties()

        _mariadbConnectionProperties.put(
            "user",
            configLoader.getString("mariadb", "username")
        )

        _mariadbConnectionProperties.put(
            "password",
            configLoader.getString("mariadb", "password")
        )

        return _mariadbConnectionProperties
    }

    def getDfFromMariadb(spark: SparkSession, table: String): DataFrame = {
        return spark.read.jdbc(this.getMariadbUrl(), table, this.getMariadbConnectionProperties())
    }

    def getDfFromMariadbWithQuery(spark: SparkSession, query: String, numPartitions: Int): DataFrame = {
        return spark.read.format("jdbc")
          .option("url", this.getMariadbUrl())
          .option("numPartitions", numPartitions)
          //          .option("partitionColumn", primaryKey)
          .option("user", this.getMariadbUser())
          .option("password", this.getMariadbPassword())
          .option("query", query)
          .load()
    }

    def execSqlToMariadb(sql: String): Unit = {
        val conn = this.getConn()

        conn.createStatement().execute(sql)

        conn.commit()

        conn.close()
    }

    def saveToMariadb(df: DataFrame, table: String, numExecutors: Int): Unit = {
        val mariadbUrl = configLoader.getString("mariadb", "conn_str")
        val mariadbConnectionProperties = new Properties()

        mariadbConnectionProperties.put(
            "user",
            configLoader.getString("mariadb", "username")
        )

        mariadbConnectionProperties.put(
            "password",
            configLoader.getString("mariadb", "password")
        )

        val sqlPrefix =
            "REPLACE INTO " + table +
              "(" + df.columns.mkString(",") + ")" +
              " VALUES "

        val batchSize = 2000
        val repartitionSize = numExecutors

        df.rdd.repartition(repartitionSize).foreachPartition{

            partition => {

                val conn = DriverManager.getConnection(
                    mariadbUrl,
                    mariadbConnectionProperties)

                conn.setAutoCommit(false)

                var count = 0
                var sql = sqlPrefix

                partition.foreach { r =>
                    count += 1

                    val values = r.mkString("'", "','", "'")
                                  .replaceAll("'null'", "null")
                                  .replaceAll("''", "null")

                    sql = sql + "(" + values + ") ,"

                    if(count == batchSize){
//                        println("寫入Mariadb筆數 : " + count)
//                        println("sql : " + sql.substring(0, sql.length - 1))
                        conn.createStatement().execute(sql.substring(0, sql.length - 1))
                        count = 0
                        sql = sqlPrefix

                        conn.commit()
                    }
                }

                if(count > 0) {
//                    println("寫入Mariadb筆數 : " + count)
//                    println("sql : " + sql.substring(0, sql.length - 1))
                    conn.createStatement().execute(sql.substring(0, sql.length - 1))
                }

                conn.commit()

                conn.close()
            }
        }

    }

}
