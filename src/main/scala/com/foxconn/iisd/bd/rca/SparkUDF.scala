package com.foxconn.iisd.bd.rca

import java.text.SimpleDateFormat

import com.foxconn.iisd.bd.rca.XWJKernelEngine.{configLoader, ctrlCCode, ctrlDCode, ctrlDValue}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.Seq

object SparkUDF{

    //取split最後一個element
    def getLast = udf((xs: Seq[String]) => (xs.last))
    //取得測試樓層與線體對應表
    def getFloorLine = udf {
        s: String =>
            configLoader.getString("test_floor_line", "code_"+s)
    }

    //parse array to string
    def parseArrayToString = udf {
        itemValue: Seq[String] => {
            itemValue.map {
                _.replace(ctrlDCode, ctrlDValue)
                .mkString("'", "", "'")
            }
        }.mkString(",")
    }

    //parse string to json string
    def parseStringToJSONString = udf {
        itemValue: Seq[String] => {
            itemValue.map {
                ele => {
                    var newString = ""
                    val eleArray = ele.split(ctrlCCode)
                    eleArray.map {
                        newEleArray => {
                            newString = newString + newEleArray.mkString("\"", "", "\"")
                            //('{"id": "d78236", "name": "Arthur Read"}')
                            if (eleArray.indexOf(newEleArray) == 0) {//nEleArray.indexOf(ctrlDCode) != -1
                               newString = newString + ":"
                            }
                        }
                        if (eleArray.size == 1) {
                            newString = newString + null
                        }
                    }
                    newString.replace(ctrlDCode, ctrlDValue)
                }
            }.mkString("{", ",", "}")
        }
    }

    //parse data type
    def castColumnDataType = udf{
        (col: String)  => {
            var datatype = "string"
            if (col != null && !col.equals("null")) {
                try {
                    if (col.indexOf(".") > 0) {
                        //float
                        val value = col.toFloat
                        if (value.isInstanceOf[Float]) {
                            datatype = "float"
                        }
                    } else {
                        //int
                        val value = col.toInt
                        if (value.isInstanceOf[Int]) {
                            datatype = "int"
                        }
                    }

                } catch {
                    case ex: Exception => {
//                        ex.printStackTrace()
                    }
                }

                if(datatype.equals("string")){//timestamp
                    if(IoUtils.convertToDate(col))
                        datatype = "timestamp"
                }
            }
            datatype
        }
    }

    //parse test item spec and value
    def parseColumnValue = udf {
        (colValue: String) => {
            var resultValue = colValue
            if(resultValue != null){
                if(resultValue.contains(ctrlDCode))
                    resultValue = resultValue.split(ctrlDCode)(1)
                if(resultValue.contains(ctrlCCode)){
                    val result = resultValue.split(ctrlCCode)
                    if(result.size == 1){
                        resultValue = null
                    }
                    else
                        resultValue = result(1)
                }
            }
            resultValue
        }
    }

    //get test detail data of first/last
//    def getTestDetailOfFirstOrLast = udf {
//        (selectColumn: String, product: String, station_name: String, sn: String) => {
//            "select " + selectColumn + " from test_detail where product='" + product + "' and station_name='" + station_name + "' and sn='" +
//                sn + "' and (value_rank='first' or value_rank='last')"
//        }
//    }
}
