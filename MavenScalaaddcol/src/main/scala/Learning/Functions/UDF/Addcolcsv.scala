package Learning.Functions.UDF

import org.apache.spark.sql.functions.{col,lit,when}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.SparkSession



object Addcolcsv extends App {
    val spark = SparkSession.builder().appName("SparkByExamples.com").master("local").getOrCreate()
    import spark.sqlContext.implicits._

    val data = Seq(("111",50000),("222",60000),("333",40000))
    val df = data.toDF("EmpId","Salary")
    df.show(false)

    df.withColumn("CopiedColumn",df("salary")* -1)
      .show(false)

    df.select($"EmpId",$"Salary", ($"salary"* -1).as("CopiedColumn") )
      .show(false)

    val df2 = df.select(col("EmpId"),col("Salary"),lit("1").as("lit_value1"))
    df2.show()

    val df3 = df2.withColumn("lit_value2",
      when(col("Salary") >=40000 && col("Salary") <= 50000, lit("100").cast(IntegerType))
        .otherwise(lit("200").cast(IntegerType))
    )
    df3.show(false)

    df3.printSchema()
    df3.show()
}
