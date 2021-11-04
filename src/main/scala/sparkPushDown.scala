package org.example.pushdown

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog

object sparkPushDown extends App {

  val sparkConf = new SparkConf()
  val url = "jdbc:postgresql://" + "localhost" + ":5432/postgres?user=test_user&password=test_password"

  sparkConf.set("spark.driver.bindAddress", "localhost")
  sparkConf.set("spark.eventLog.enabled", "true")
    .set("spark.sql.catalog.postgresql", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.postgresql.url", url)
    .set("spark.sql.catalog.postgresql.driver", "org.postgresql.Driver")
    .set("spark.sql.catalog.postgresql.pushDownAggregate", "true")


  val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("SPARK_PUSH_DOWN_OPERATORS")
      .config(sparkConf)
      .getOrCreate()

  import org.apache.spark.sql.functions._

  // scan with aggregate push-down: MAX without filter and without group by
  val df1 = spark
    .read
    .table("postgresql.test.employee")
    .agg(max("salary"))

  df1.explain()
  df1.show()

  // scan with aggregate push-down: MAX MIN with filter without group by
  val df2 = spark.sql("select MAX(SALARY), MIN(SALARY) FROM postgresql.test.employee where DEPT > 0")
  df2.explain()
  df2.show()

  // stop spark
  spark.stop()

}
