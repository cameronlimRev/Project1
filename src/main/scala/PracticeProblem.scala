import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkContext, SparkConf}
import java.math._

object PracticeProblem {
  def main(args:Array[String]): Unit ={
    System.setProperty("hadoop.home.dir", "C:\\Hadoop")

    val spark = SparkSession.builder()
      .appName("I wonder if this matters")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df =spark.read.option("multiline","true").json("input/bank_edited.json")
    val test1 = df.count().toDouble
    var res = df.select("y").where("y='no'").count().toDouble
    val finalRes = (res/test1)*100
    println("The failure rate is: " + finalRes)

    println("The maximum age of a client is: ")
    val res2 = df.select("age").orderBy(desc("age")).show(1)
    println("The minimum age of a client is: ")
    val res3 = df.select("age").orderBy(asc("age")).show(1)
    println("The average age of a client is: ")
    val res4 = df.select(round(avg("age"), 2)).withColumnRenamed("round(avg(age), 2)", "Average Age").show()

    println("The average balance of a client is: ")
    val avgBalance = df.select(round(avg("balance"), 2)).withColumnRenamed("round(avg(balance), 2)", "Average Balance").show()
    val medianArray = df.stat.approxQuantile("balance", Array(0.5), 0)
    println("Median balance is " + medianArray(0))

    val res5 = df.select("y").where("(age > 0 AND age < 50) AND y='no'").count().toDouble
    println("The amount of clients under 50 with 'No': " + res5)
    val res6 = df.select("y").where("(age > 0 AND age < 50) AND y='yes'").count().toDouble
    println("The amount of clients under 50 with 'Yes': " + res6)
    val res7 = df.select("y").where("(age > 50 AND age < 100) AND y='no'").count().toDouble
    println("The amount of clients over 50 with 'No': " + res7)
    val res8 = df.select("y").where("(age > 50 AND age < 100) AND y='yes'").count().toDouble
    println("The amount of clients over 50 with 'Yes': " + res8)
    val res9 = ((res6/(res5+res6))*100)
    val res10 = ((res8/(res7+res8))*100)
    println(f"The percentage of clients under 50 who signed yes: $res9%1.2f%%")
    println(f"The percentage of clients over 50 who signed yes: $res10%1.2f%%")
    println()
    println()

    val res11 = df.select("y").where("(marital='single') AND y='no'").count().toDouble
    println("The amount of clients single with 'No': " + res11)
    val res12 = df.select("y").where("(marital='single') AND y='yes'").count().toDouble
    println("The amount of clients single with 'Yes': " + res12)
    val res13 = df.select("y").where("(marital='married') AND y='no'").count().toDouble
    println("The amount of clients married with 'No': " + res13)
    val res14 = df.select("y").where("(marital='married') AND y='yes'").count().toDouble
    println("The amount of clients married with 'Yes': " + res14)
    val res15 = df.select("y").where("(marital='divorced') AND y='no'").count().toDouble
    println("The amount of clients divorced with 'No': " + res15)
    val res16 = df.select("y").where("(marital='divorced') AND y='yes'").count().toDouble
    println("The amount of clients divorced with 'Yes': " + res16)
    val res17 = ((res12/(res11+res12))*100)
    val res18 = ((res14/(res13+res14))*100)
    val res19 = ((res16/(res15+res16))*100)
    println(f"The percentage of clients single who signed yes: $res17%1.2f%%")
    println(f"The percentage of clients married who signed yes: $res18%1.2f%%")
    println(f"The percentage of clients divorced who signed yes: $res19%1.2f%%")
  }
}
