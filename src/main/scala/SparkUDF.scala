package com.foxconn.iisd.bd.rca

import java.time.format.DateTimeFormatter
import java.util.Locale

import com.foxconn.iisd.bd.rca.XWJKernelEngine.configLoader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.Seq

object SparkUDF{

    //取split最後一個element
    def getLast = udf((xs: Seq[String]) => (xs.last))
    //取得測試樓層與線體對應表
    def getFloorLine = udf {
        s: String =>
            configLoader.getString("test_floor_line", "code_"+s)
    }
    //parse array to json
    def parseArrayToJSON = udf {
        itemValue: Seq[String] =>{
            itemValue.map{ _.split("\003") }

/*
            item.foldLeft("") { (key:String, idx:String) =>
                key match {
                    case _ => {
                        println(key)
                        println(idx)
                        key + ":" + itemValue(idx.toInt) + ";"
                    }
                }
            }.mkString(",")
*/
            /*
            item.map({
                ele =>
                    var idx = 0
                    println(idx)
                    var group = ele +  ":" + value(idx) + ";"
                    allStr = allStr + group
                    idx = idx + 1
            }).mkString(",")*/
            /*item.foldLeft("")
            { (ele, idx) =>
                var group = ele + ":" + value(idx.toInt) +";"
                allStr = allStr + group
            }*/

            //allStr = "{" + allStr + "}"
        }

    }

}
