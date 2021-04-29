package Learning
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Sparkhellow{
   // case class Employee(Timestamp:String, Age:String, Gender:String, Country:String, state:String, self_employed:String , family_history:String)
    case class Employee(empno:String, ename:String, designation:String, manager:String, hire_date:String, sal:String , deptno:String)
    def main(args : Array[String]): Unit = {
        var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val textRDD = sc.textFile("src\\main\\resources\\emp_data.csv")
       // println(textRDD.foreach(println))
        val empRdd = textRDD.map {
            line =>
                val col = line.split(",")
                Employee(col(0), col(1), col(2), col(3), col(4), col(5), col(6))
        }
        val empDF = empRdd.toDF()
        empDF.show()

    }
}
