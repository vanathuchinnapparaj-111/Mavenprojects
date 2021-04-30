package Learning.Functions.UDF

import Learning.Functions.UDF.Addcsv.Employee
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{current_date, current_timestamp, from_unixtime, unix_timestamp}
import org.apache.spark.sql.types.TimestampType

object ConvertTimestamp {
  case class Employee(empno:String, ename:String, designation:String, manager:String, hire_date:String, sal:String , deptno:String)
  def main(args:Array[String]): Unit ={
    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val textRDD = sc.textFile("src\\main\\resources\\emp_data.csv")
    println(textRDD.foreach(println))
    val empRdd = textRDD.map {
      lineval =>
        val col = lineval.split(",")
        Employee(col(0), col(1), col(2), col(3), col(4), col(5), col(6))
    }
    val valDF = empRdd.toDF()
    valDF.printSchema()
    //valDF.withColumn("Timestamp",valDF("hire_date")).show()
    valDF.withColumn("TimeStamp",from_unixtime(unix_timestamp($"hire_date".cast("string"),"yyyyMMddHHmmss"))).show()
  }
}
