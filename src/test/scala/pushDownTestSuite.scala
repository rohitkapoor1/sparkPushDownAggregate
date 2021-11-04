package org.example.pushdown

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import java.sql.{Connection, DriverManager}
import java.util.Properties

class pushDownTestSuite extends FunSuite with BeforeAndAfterAll{

  @transient var spark : SparkSession = _
  val url = "jdbc:postgresql://" + "localhost" + ":5432/postgres?user=test_user&password=test_password"

  private def withConnection[T](f: Connection => T): T = {
    val conn = DriverManager.getConnection(url, new Properties())
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }

  override def beforeAll(): Unit = {

    val sparkConf = new SparkConf()

    sparkConf.set("spark.driver.bindAddress", "localhost")
    sparkConf.set("spark.eventLog.enabled", "true")
      .set("spark.sql.catalog.postgresql", classOf[JDBCTableCatalog].getName)
      .set("spark.sql.catalog.postgresql.url", url)
      .set("spark.sql.catalog.postgresql.driver", "org.postgresql.Driver")
      .set("spark.sql.catalog.postgresql.pushDownAggregate", "true")

    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("SPARK_PUSH_DOWN_OPERATORS_TEST")
      .config(sparkConf)
      .getOrCreate()

    withConnection { conn =>
      conn.prepareStatement("create schema if not exists test_schema")
        .executeUpdate()
      conn.prepareStatement("create table if not exists test_schema.employee(empid int, ename varchar(40), sal int, dept int)")
        .executeUpdate()

      conn.prepareStatement("INSERT INTO test_schema.employee VALUES (1, 'amy', 10000, 1000)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO test_schema.employee VALUES (2, 'alex', 12000, 1200)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO test_schema.employee VALUES (1, 'cathy', 9000, 1200)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO test_schema.employee VALUES (2, 'david', 10000, 1300)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO test_schema.employee VALUES (6, 'jen', 12000, 1200)")
        .executeUpdate()

    }
  }

  override def afterAll(): Unit = {

    withConnection { conn =>
      conn.prepareStatement("delete from test_schema.employee")
        .executeUpdate()
    }
    spark.stop()
  }

  private def getNormalizedExplain(df: DataFrame): String = {
    val output = new java.io.ByteArrayOutputStream()
    Console.withOut(output) {
      df.explain("extended")
    }
    output.toString.replaceAll("#\\d+", "#x")
  }

  private def checkKeywordsExistsInExplain(df: DataFrame, keywords: String*): Unit = {
    withNormalizedExplain(df) { normalizedOutput =>
      for (key <- keywords) {
        assert(normalizedOutput.contains(key))
      }
    }
  }

  private def withNormalizedExplain(df: DataFrame)(f: String => Unit) = {
    f(getNormalizedExplain(df))
  }

  import org.apache.spark.sql.functions._

  test("scan with aggregate push-down: MAX without filter and without group by") {
    val df1 = spark
      .read
      .table("postgresql.test_schema.employee")
      .agg(max("sal"))

    assert(df1.collect().toSeq === Seq(Row(12000)))

    val expected_plan_fragment =
      "PushedAggregates: [MAX(sal)], " +
        "PushedFilters: [], " +
        "PushedGroupby: []"

    checkKeywordsExistsInExplain(df1,expected_plan_fragment)
  }

  test("scan with aggregate push-down: MAX MIN with filter without group by") {
    val df2 = spark.sql("select MAX(sal), MIN(sal) FROM postgresql.test_schema.employee where DEPT > 0")
    assert(df2.collect().toSeq === Seq(Row(12000, 9000)))

    val expected_plan_fragment =
      "PushedAggregates: [MAX(sal), MIN(sal)], " +
        "PushedFilters: [IsNotNull(dept), GreaterThan(dept,0)], " +
        "PushedGroupby: []"

    checkKeywordsExistsInExplain(df2,expected_plan_fragment)
  }

}
