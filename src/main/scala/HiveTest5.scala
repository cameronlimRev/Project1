import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.asc

import scala.io.StdIn.{readDouble, readInt, readLine}

object HiveTest5 {

    def createDatabase(spark:SparkSession): Unit = {
//        spark.sql("DROP TABLE bev_branchA")
//        spark.sql("DROP TABLE bev_countA")
        spark.sql("create table bev_branchA(id Int, beverage String, branch String) row format delimited fields terminated by ','")
        spark.sql("create table bev_countA(id Int, beverage String, consumers Int) row format delimited fields terminated by ','")
        spark.sql("LOAD DATA LOCAL INPATH 'input/branchA.txt' INTO TABLE bev_branchA")
        spark.sql("LOAD DATA LOCAL INPATH 'input/branchAcons.txt' INTO TABLE bev_countA")
//        spark.sql("DROP TABLE bev_branchB")
//        spark.sql("DROP TABLE bev_countB")
        spark.sql("create table bev_branchB(id Int, beverage String, branch String) row format delimited fields terminated by ','")
        spark.sql("create table bev_countB(id Int, beverage String, consumers Int) row format delimited fields terminated by ','")
        spark.sql("LOAD DATA LOCAL INPATH 'input/branchB.txt' INTO TABLE bev_branchB")
        spark.sql("LOAD DATA LOCAL INPATH 'input/branchBcons.txt' INTO TABLE bev_countB")
//        spark.sql("DROP TABLE bev_branchC")
//        spark.sql("DROP TABLE bev_countC")
        spark.sql("create table bev_branchC(id Int, beverage String, branch String) row format delimited fields terminated by ','")
        spark.sql("create table bev_countC(id Int, beverage String, consumers Int) row format delimited fields terminated by ','")
        spark.sql("LOAD DATA LOCAL INPATH 'input/branchC.txt' INTO TABLE bev_branchC")
        spark.sql("LOAD DATA LOCAL INPATH 'input/branchCcons.txt' INTO TABLE bev_countC")

    }

    def scenario1(spark: SparkSession): Unit = {
        // Answer
//        spark.sql("DROP TABLE IF EXISTS branch1")
//        spark.sql("create table if not exists branch1(beverage String, consumers Int)")
//        spark.sql("INSERT INTO TABLE branch1 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='SMALL_Espresso') as t) as b where row_num %2 != 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch1 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='Special_Coffee') as t) as b where row_num %2 != 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch1 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='Double_Espresso') as t) as b where row_num %2 != 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch1 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='MED_MOCHA') as t) as b where row_num %2 != 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch1 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='LARGE_cappuccino') as t) as b where row_num %3 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch1 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='Triple_MOCHA') as t) as b where row_num %2 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch1 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='Mild_LATTE') as t) as b where row_num %2 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch1 SELECT beverage, SUM(consumers) from bev_countA where beverage='ICY_Espresso' group by beverage")
//        spark.sql("INSERT INTO TABLE branch1 SELECT beverage, SUM(consumers) from bev_countA where beverage='Cold_LATTE' group by beverage")
//        spark.sql("INSERT INTO TABLE branch1 SELECT beverage, SUM(consumers) from bev_countA where beverage='SMALL_MOCHA' group by beverage")
//        spark.sql("INSERT INTO TABLE branch1 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='Special_cappuccino') as t) as b where row_num %3 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch1 SELECT beverage, SUM(consumers) from bev_countA where beverage='Double_MOCHA' group by beverage")
//        spark.sql("INSERT INTO TABLE branch1 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='MED_LATTE') as t) as b where row_num %2 != 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch1 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='LARGE_Espresso') as t) as b where row_num %2 != 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch1 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='Triple_LATTE') as t) as b where row_num %2 != 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch1 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='Mild_Lite') as t) as b where row_num %2 != 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch1 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='ICY_MOCHA') as t) as b where row_num %2 != 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch1 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='Cold_Lite') as t) as b where row_num %2 != 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch1 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='SMALL_LATTE') as t) as b where row_num %2 != 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch1 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='Special_Espresso') as t) as b where row_num %2 != 0 group by beverage")

//        spark.sql("INSERT OVERWRITE TABLE  branch1 SELECT id, first(beverage), SUM(consumers) from bev_countA group by id")
        println("These are the following consumed drinks in Branch 1:")
        spark.sql("SELECT * from branch1").show()
//        spark.sql("DROP TABLE IF EXISTS branch2")
//        spark.sql("create table if not exists branch2(beverage String, consumers Int)")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Triple_LATTE') as t) as b where row_num %5 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Special_Coffee') as t) as b where row_num %5 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='MED_cappuccino') as t) as b where row_num %4 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Double_cappuccino') as t) as b where row_num %4 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='ICY_LATTE') as t) as b where row_num %5 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='SMALL_cappuccino') as t) as b where row_num %2 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Cold_MOCHA') as t) as b where row_num %5 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='LARGE_cappuccino') as t) as b where row_num %4 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Mild_Lite') as t) as b where row_num %5 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Triple_Lite') as t) as b where row_num %4 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Special_cappuccino') as t) as b where row_num %7 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='MED_Coffee') as t) as b where row_num %4 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Double_Coffee') as t) as b where row_num %2 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='ICY_Lite') as t) as b where row_num %2 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='SMALL_Espresso') as t) as b where row_num %2 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Cold_LATTE') as t) as b where row_num %5 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='LARGE_Coffee') as t) as b where row_num %5 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Mild_cappuccino') as t) as b where row_num %3 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Triple_cappuccino') as t) as b where row_num %3 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Special_Espresso') as t) as b where row_num %3 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='MED_cappuccino') as t) as b where row_num %4 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Double_cappuccino') as t ) as b where row_num %2 != 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='ICY_cappuccino') as t) as b where row_num %4 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='SMALL_MOCHA') as t) as b where row_num %2 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Cold_Lite') as t) as b where row_num %2 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='LARGE_cappuccino') as t) as b where row_num %2 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Mild_Coffee') as t) as b where row_num %3 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Triple_Coffee') as t) as b where row_num %3 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Special_MOCHA') as t) as b where row_num %3 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='MED_Espresso') as t) as b where row_num %3 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Double_Espresso') as t) as b where row_num %7 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='ICY_Coffee') as t) as b where row_num %3 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='SMALL_LATTE') as t) as b where row_num %3 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Cold_cappuccino') as t) as b where row_num %6 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='LARGE_Espresso') as t) as b where row_num %3 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Mild_cappuccino') as t) as b where row_num %3 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Triple_cappuccino') as t) as b where row_num %3 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Special_LATTE') as t) as b where row_num %3 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='MED_MOCHA') as t) as b where row_num %4 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Double_MOCHA') as t) as b where row_num %4 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='SMALL_Lite') as t) as b where row_num %4 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='ICY_cappuccino') as t) as b where row_num %4 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Cold_Coffee') as t) as b where row_num %4 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='LARGE_MOCHA') as t) as b where row_num %4 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Mild_Espresso') as t) as b where row_num %4 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Triple_Espresso') as t) as b where row_num %4 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Special_Lite') as t) as b where row_num %5 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='MED_LATTE') as t) as b where row_num %4 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Double_LATTE') as t) as b where row_num %4 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='ICY_Espresso') as t) as b where row_num %4 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='SMALL_cappuccino') as t) as b where row_num %9 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Cold_cappuccino') as t) as b where row_num %9 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='LARGE_LATTE') as t) as b where row_num %5 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Mild_MOCHA') as t) as b where row_num %5 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Triple_MOCHA') as t) as b where row_num %5 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Special_cappuccino') as t) as b where row_num %9 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='MED_Lite') as t) as b where row_num %5 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='Double_Lite') as t) as b where row_num %5 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='ICY_MOCHA') as t) as b where row_num %5 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countC where beverage='SMALL_Coffee') as t) as b where row_num %5 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='MED_LATTE') as t) as b where row_num %2 != 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='LARGE_Espresso') as t) as b where row_num %2 != 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='Triple_LATTE') as t) as b where row_num %2 != 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='Mild_Lite') as t) as b where row_num %2 != 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='ICY_MOCHA') as t) as b where row_num %2 != 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='Cold_Lite') as t) as b where row_num %2 != 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='SMALL_LATTE') as t) as b where row_num %2 != 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='Special_Espresso') as t) as b where row_num %2 != 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='Double_LATTE') as t) as b where row_num %1 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='MED_Lite') as t) as b where row_num %1 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='LARGE_MOCHA') as t) as b where row_num %1 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='Triple_Lite') as t) as b where row_num %1 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='Mild_cappuccino') as t) as b where row_num %2 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='ICY_LATTE') as t) as b where row_num %2 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='Cold_cappuccino') as t) as b where row_num %3 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='SMALL_Lite') as t) as b where row_num %2 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='Special_MOCHA') as t) as b where row_num %2 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='Double_Lite') as t) as b where row_num %2 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='MED_cappuccino') as t) as b where row_num %3 = 0 group by beverage")
//        spark.sql("INSERT INTO TABLE branch2 SELECT beverage, SUM(consumers) from (select * from (select row_number() OVER (order by id) AS row_num, beverage, consumers from bev_countA where beverage='LARGE_LATTE') as t) as b where row_num %2 = 0 group by beverage") // //CReate
        println("These are the following consumed drinks in Branch 2:")
        spark.sql("SELECT beverage, sum(consumers) from branch2 GROUP BY beverage").show(100)
        println("This is the total number of consumers for branch1: ")
        spark.sql("SELECT SUM(consumers) as Total_Consumers_Branch1 from branch1").show()
        println("This is the total consumer count for branch2: ")
        spark.sql("SELECT SUM(consumers) as Total_Consumers_Branch2 from branch2").show()
    }

    def scenario2(spark: SparkSession): Unit = {
        println("The most consumed beverage in branch 1: ")
        spark.sql("SELECT beverage, consumers from branch1 ORDER BY consumers DESC").show(1)
        println("The least consumed beverage in branch 2: ")
        spark.sql("SELECT beverage, sum_cons from (SELECT beverage, SUM(consumers) as sum_cons from branch2 GROUP BY beverage) ORDER BY sum_cons ASC").show(1)
        println("The average amount of beverages consumed in branch 2: ")
        spark.sql("SELECT AVG(sum_cons) as Average_Consumers FROM (SELECT beverage, SUM(consumers) as sum_cons from branch2 GROUP BY beverage)").show(1)
    }

    def scenario3(spark: SparkSession): Unit = {
        println("The beverages available on Branches 1,8, and 9 are:  ")
        spark.sql("SELECT DISTINCT bev_branchA.beverage from bev_branchA JOIN bev_branchB ON bev_branchA.beverage = bev_branchB.beverage AND (bev_branchA.branch='Branch1' OR bev_branchA.branch='Branch8' OR bev_branchA.branch='Branch9') JOIN bev_branchC ON bev_branchA.beverage = bev_branchC.beverage AND (bev_branchA.branch='Branch1' OR bev_branchA.branch='Branch8' OR bev_branchA.branch='Branch9')").show(100)
        println("The common beverages between Branch 4 and Branch 7 are: ")
        spark.sql("SELECT DISTINCT bev_branchB.beverage from bev_branchB where (branch='Branch4' OR branch='Branch7') INTERSECT (SELECT bev_branchC.beverage from bev_branchC where (branch='Branch4' OR branch='Branch7'))").show(100)
        println("Extra information to help understand and verify the joins and intersections: ")
        spark.sql("select count(*) as Number_of_Similar_Beverages from (SELECT DISTINCT bev_branchB.beverage from bev_branchB where (branch='Branch4' OR branch='Branch7') INTERSECT (SELECT bev_branchC.beverage from bev_branchC where (branch='Branch4' OR branch='Branch7')))").show(100)
        spark.sql("select count(DISTINCT beverage) as Total_Beverage_BranchA FROM bev_branchA").show(100)
        spark.sql("select count(DISTINCT beverage) as Total_Beverage_BranchB FROM bev_branchB").show(100)
        spark.sql("select count(DISTINCT beverage) as Total_Beverage_BranchC FROM bev_branchC").show(100)
    }

    def scenario4(spark: SparkSession): Unit = {
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

        spark.sql("DROP TABLE IF EXISTS scenario4_table")
        println("Creating partitioned table...")
        spark.sql("CREATE TABLE scenario4_table(id INT, beverage String, branch String) PARTITIONED BY (branch)")
        println("Inserting data into our table...")
        spark.sql("INSERT INTO scenario4_table SELECT * from bev_branchA")
        spark.sql("INSERT INTO scenario4_table SELECT * from bev_branchB")
        spark.sql("INSERT INTO scenario4_table SELECT * from bev_branchC")
        println("Printing our partitioned table...")
        spark.sql("SELECT * from scenario4_table").show(1000)
        spark.sql("desc scenario4_table").show()
        println("The beverages available on Branches 1,8, and 9 are (from partitioned table):  ")
        spark.sql("Select DISTINCT beverage from scenario4_table where (branch='Branch1' OR branch='Branch8' OR branch='Branch9')").show(1000)

        spark.sql("DROP VIEW IF EXISTS scenario3_view")
        spark.sql("CREATE VIEW scenario3_view AS SELECT DISTINCT bev_branchB.beverage from bev_branchB where (branch='Branch4' OR branch='Branch7') INTERSECT (SELECT bev_branchC.beverage from bev_branchC where (branch='Branch4' OR branch='Branch7'))")
        println("View has been created.")
        spark.sql("SELECT * from scenario3_view").show(100)
    }

    def scenario5(spark: SparkSession): Unit = {
        val result = readLine("Enter the comment you want to add to scenario 4's table: ")
        spark.sql(s"COMMENT ON TABLE scenario4_table is '$result'")
        println("Table altered to add comment.")

//        println("Please enter an ID (1-200) to remove:")
//        val result2 = readInt()
//        spark.sql(s"DELETE FROM scenario4_table WHERE id=$result2")
//        spark.sql("select * from scenario4_table").show(1000)
    }

    def openMenu(): Int = {
        println(
            s"""  _______________________________________
               |//   Welcome to Project 1 by Cameron Lim  \\\\
               ||| ========================================||
               ||| 1. Scenario 1                           ||
               ||| 2. Scenario 2                           ||
               ||| 3. Scenario 3                           ||
               ||| 4. Scenario 4                           ||
               ||| 5. Scenario 5                           ||
               ||| 6. Scenario 6                           ||
               ||| 7. Exit                                 ||
               |\\\\_________________________________________//
               |""".stripMargin)
        println("Please enter a number (1-8) to continue: ")
        val result = readInt()
        return result
    }

    def checkAction(filter: Int, spark: SparkSession): Boolean = {
        filter match {
            case 1 => {
                scenario1(spark)
                true
            }
            case 2 => {
                scenario2(spark)
                true
            }
            case 3 => {
                scenario3(spark)
                true
            }
            case 4 => {
                scenario4(spark)
                true
            }
            case 5 => {
                scenario5(spark)
                true
            }
//            case 6 => {
//                try {
//                    scenario6(spark)
//                    }
//                } catch {
//                    case e: Exception => e.printStackTrace
//                }
//                true
           // }
            case 7 => {
                println("Goodbye!")
                false
            }
            case _ => {
                println("Error: Please try again. Enter a number (1-7) ")
                val result = readInt()
                true
            }
        }
    }

    def checkContinue(): Boolean = {
        val result = readLine("Please enter \"Yes\" if you need to continue: ")
        if (result.equals("Yes") || result.equals("yes")){
            true
        }else
            false
    }

    def main(args: Array[String]): Unit = {

        //===================================================================================================================================
        //EXAMPLES
        // create a spark session
        // for Windows
        System.setProperty("hadoop.home.dir", "C:\\winutils")

        val spark = SparkSession.builder()
          .appName("I wonder if this matters")
          .config("spark.master", "local")
          .enableHiveSupport()
          .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        println("Starting testing for Project 1")
        var start = true
        var action = 1
        action = openMenu()
        while(start) {
            start = checkAction(action, spark)
            if (start == false){
                System.exit(0)
            }
            start = checkContinue()
            if (start == true){
                action = openMenu()
            }
        }
    }
    //adding a comment for git
}