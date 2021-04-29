package Learning
import org.apache.spark.{SparkConf, SparkContext}

object SamplefindMax {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Emp Dept Assignment").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val emp_data = sc.textFile("src\\main\\resources\\emp_data.txt")
    println(emp_data.foreach(println))
    val emp_header = emp_data.first()
    println(emp_header)
    val emp_data_without_header = emp_data.filter(line => !line.equals(emp_header))

    println(emp_data_without_header.foreach(println))
    println("No. of partition = " + emp_data_without_header.partitions.size)
    val emp_salary_list = emp_data_without_header.map{x => x.split(',')}.map{x => (x(5).toDouble)}
    println("Highest salaty:"+ emp_salary_list.max())
    val max_salary = emp_salary_list.distinct.sortBy(x => x.toDouble, false, 1)
    print(max_salary.take(1).foreach(println))
    val min_salary = emp_salary_list.distinct.sortBy(x => x.toDouble, true, 1)
    print(min_salary.take(1).foreach(println))
    val second_highest_salary = max_salary.zipWithIndex().filter(index => index._2 == 1).map(_._1)
    print(second_highest_salary.foreach(println))
    val second_min_salary = min_salary.zipWithIndex().filter(index => index._2 == 1).map(_._1)
    print(second_min_salary.foreach(println))
    val salaryWithEmployeeName = emp_data_without_header.map{x => x.split(',')}.map{x => (x(5).toDouble, x(1))}
    val maxSalaryEmployee = salaryWithEmployeeName.groupByKey.takeOrdered(1)(Ordering[Double].reverse.on(_._1))
    print(maxSalaryEmployee.foreach(println))
  }
}
