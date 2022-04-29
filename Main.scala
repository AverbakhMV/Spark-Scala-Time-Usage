package ru.filit.bigdata.timeusage

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import java.io.File

object Main extends App {

  def main() : Unit = {
    implicit val spark : SparkSession= SparkSession
      .builder()
      .master("local[*]")
      .appName("timeUsage")
      .getOrCreate()

    val STRUCT_TYPE: StructType = StructType(Array(
      StructField("ID", StringType, nullable = false),
      StructField("SEX", StringType, nullable = false),
      StructField("AGE", IntegerType, nullable = true),
      StructField("BASE", IntegerType, nullable = true),
      StructField("WORK", IntegerType, nullable = true),
      StructField("LEISURE", IntegerType, nullable = true)
    ))
    val table = loadTable()
    val header = headers(table)
    val tableWithoutHeader = tableWithoutHeaders(table)
    val ind = indexes(header)
    val resultRdd = resultRDD(tableWithoutHeader, ind)
    val df = createDataFrame(resultRdd, STRUCT_TYPE)
    val grouped = groupedDf(df).cache()
    val result =  results(grouped)
    printResult(result)
  }

  /*
  * create a path to file with data
  * @return path
   */
  def filePath: String = {
    val resource = this.getClass.getClassLoader.getResource("timeusage/atussum.csv")
    if (resource == null) sys.error("Please download the dataset as explained in the assignment instructions")
    new File(resource.toURI).getPath
  }

  /*
  * load table from csv file to RDD
  * @return RDD
   */
  def loadTable()(implicit spark: SparkSession): RDD[String] = {
    spark.sparkContext.textFile(filePath)
  }

  /*
  * remove headers from RDD
  * @param table RDD with headers
  * @return RDD without headers
   */
  def tableWithoutHeaders(table: RDD[String]): RDD[String] =
    table.mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter)

  /*
  * get headers from RDD
  * @param table - RDD
  * @return list of columns headers
   */
  def headers(table: RDD[String]): List[String] = {
    table.first().split(",").toList
  }

  /*
  * check if the header corresponds base activity
  * @param s - string with column header
  * return true if it's a base activity header
  */
  def checkBase(s: String): Boolean = {
    s.startsWith("t01") ||
      s.startsWith("t03") ||
      s.startsWith("t11") ||
      s.startsWith("t1801") ||
      s.startsWith("t1803")
  }

  /*
  * check if the header corresponds work activity
  * @param s - string with column header
  * return true if it's a work activity header
  */
  def checkWork(s: String): Boolean = {
    s.startsWith("t05") ||
    s.startsWith("t1805")
  }

  /*
  * check if the header corresponds leisure activity
  * @param s - string with column header
  * return true if it's a work leisure header
  */
  def checkLeisure(s: String): Boolean = {
    s.startsWith("t02") ||
      s.startsWith("t04") ||
      s.startsWith("t06") ||
      s.startsWith("t07") ||
      s.startsWith("t08") ||
      s.startsWith("t09") ||
      s.startsWith("t10") ||
      s.startsWith("t12") ||
      s.startsWith("t13") ||
      s.startsWith("t14") ||
      s.startsWith("t15") ||
      s.startsWith("t16") ||
      s.startsWith("t18") &&
        !(s.startsWith("t1801") ||
          s.startsWith("t1803") ||
          s.startsWith("t1805"))
  }

  /*
  * find column indexes that correspond ID, sex, age, base, work and leisure activities
  * @param headers - List of column headers
  * @return list of indexes
   */
  def indexes(headers: List[String]): List[List[Int]] = {
    val idIndex = headers.filter(x => x.startsWith("tucaseid")).map(x => headers.indexOf(x))
    val sexIndex = headers.filter(x => x.startsWith("tesex")).map(x => headers.indexOf(x))
    val ageIndex = headers.filter(x => x.startsWith("teage")).map(x => headers.indexOf(x))
    val baseIndexes = headers.filter(x => checkBase(x)).map(x => headers.indexOf(x))
    val workIndexes = headers.filter(x => checkWork(x)).map(x => headers.indexOf(x))
    val leisureIndexex = headers.filter(x => checkLeisure(x)).map(x => headers.indexOf(x))
    List(idIndex, sexIndex, ageIndex, baseIndexes, workIndexes, leisureIndexex)
  }

  /*
  * divide ages by 3 categories (<23, 23 to 55, >55)
  * @param age - the value of age column
  * @return - 1,2 or 3 category
   */
  def ageGroup(age: Int): Int = {
    age match {
      case x if x < 23 => 1
      case x if x < 56 => 2
      case _ => 3
    }
  }

  /*
  * define respondent's sex
  * @param sex - the value of sex column
  * @return - №(n-1) for sex 1 and №(n-2) for sex 2
 */
  def sex(sex: Int): String = if (sex == 1) "№(n-1)" else "№(n-2)"

  /*
  * create RDD that contains respondent's id, sex, age group, hours for base, work and leisure activities
  * from origin RDD
  * @param table - origin RDD
  * @param indexes - List of necessary column' indexes
  * @return new RDD
  */
  def resultRDD(table: RDD[String], indexes: List[List[Int]]) : RDD[Row] = {
    table.map(x => List(x.split(',')(indexes(0)(0)),
      sex(x.split(',')(indexes(1)(0)).toInt),
      ageGroup(x.split(',')(indexes(2)(0)).toInt),
      indexes(3).map(index => x.split(',')(index).toInt).sum/60,
      indexes(4).map(index => x.split(',')(index).toInt).sum/60,
      indexes(5).map(index => x.split(',')(index).toInt).sum/60))
      .map{case List(a,b,c,d,e,f) => Row(a,b,c,d,e,f)}
  }

  /*
  * create DataFrame from RDD
  * @param RDD - RDD
  * @param schema - DataFrame schema
  * @return DataFrame
   */
  def createDataFrame(rdd: RDD[Row], schema: StructType)(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(rdd, schema)
  }

  /*
  * udf that define if the respondent is employed or not
  * @param work - average work hours a day
  * @return 'unemployed' if the average work hours a day less than 2, 'employed' otherwise
   */
  def employed = udf((work: Int) => {
    if (work < 2) "unemployed" else "employed"
  })

  /*
  * add employed column to DataFrame, calculate average hours for base, work and leisure activities
  * by sex, age and employment status
  * @param df - origin DataFrame
  * @return new DataFrame
   */
  def groupedDf(df: DataFrame): DataFrame ={
    val empDf = df.withColumn("EMPLOYED", employed(col("WORK")))
    empDf.groupBy("SEX", "AGE", "EMPLOYED")
      .agg(avg("BASE") as "BASE",
          avg("WORK") as "WORK",
          avg("LEISURE") as "LEISURE")
      .sort("SEX", "AGE", "EMPLOYED")
  }

  /*
  * calculate
  * 1. the time we spend for base needs comparing with other activities
  * 2. hours that (n-1) and (n-2) sexes spend for work
  * 3. the time people spend for base needs depending on age group
  * 4. the time that employed and unemployed people spend for leisure
  * @param df = grouped DataFrame
  * @return list of parameters, necessary to answer the questions
   */
  def results(df: DataFrame): List[Double] = {

    // average time people spend for base needs and its percent of all activities
    val basePercent = df.select(avg("BASE") as "HOURS_FOR BASE_NEEDS",
      avg("BASE") * 100 / (avg("WORK") + avg("LEISURE") + avg("BASE")) as "PERCENT_OF_BASE_NEEDS")
      .collectAsList()

    val baseHours = basePercent.get(0).toSeq.asInstanceOf[Seq[Double]](0)
    val basePerc = basePercent.get(0).toSeq.asInstanceOf[Seq[Double]](1)

    // time people spend on work depending on sex
    val workBySex = df.filter(col("EMPLOYED") === "employed")
        .groupBy("SEX")
        .agg(avg("WORK") as "WORK_HOURS")
        .select("SEX", "WORK_HOURS")
        .sort("SEX")
        .collectAsList()

    val sex1Hours = workBySex.get(0).toSeq.asInstanceOf[Seq[Double]](1)
    val sex2Hours = workBySex.get(1).toSeq.asInstanceOf[Seq[Double]](1)

    // time people spend on base needs depending on age group
    val ageBase = df.groupBy("AGE")
      .agg(avg("BASE") as "BASE_HOURS")
      .select("AGE","BASE_HOURS")
      .sort("AGE")
      .collectAsList()

    val age1Base = ageBase.get(0).toSeq.asInstanceOf[Seq[Double]](1)
    val age2Base = ageBase.get(1).toSeq.asInstanceOf[Seq[Double]](1)
    val age3Base = ageBase.get(2).toSeq.asInstanceOf[Seq[Double]](1)

    //time people spend on leisure depending on employment status
    val leisure = df.groupBy("EMPLOYED")
      .agg(avg("LEISURE") as "LEISURE_HOURS")
      .select("EMPLOYED", "LEISURE_HOURS")
      .sort("EMPLOYED")
      .collectAsList()

    val les1 = leisure.get(0).toSeq.asInstanceOf[Seq[Double]](1)
    val les2 = leisure.get(1).toSeq.asInstanceOf[Seq[Double]](1)

    List(baseHours, basePerc, sex1Hours, sex2Hours, age1Base, age2Base, age3Base, les1, les2)
  }

  /*
  * print results
  * @param results - list of results
  * @return nothing
  */
  def printResult(results: List[Double]): Unit ={
    val bh = results(0)
    val perc = results(1)
    val sex1 = results(2)
    val sex2 = results(3)
    val age1 = results(4)
    val age2 = results(5)
    val age3 = results(6)
    val les1 = results(7)
    val les2 = results(8)

    println(f"average person spends $bh%.1f hours a day for base needs, this is $perc%.1f percent of all activities")
    println("============================================================================================================")
    println(f"among employed people genre №(n-1) spends $sex1%.1f hours a day for work whereas genre №(n-2) spends $sex2%.1f hours")
    println("============================================================================================================")
    println(f"people till 23 y.o. spend $age1%.1f hours a day for base needs")
    println(f"people from 23 to 55 y.o. spend $age2%.1f hours a day for base needs")
    println(f"people older than 55 y.o. spend $age3%.1f hours a day for base needs")
    println("============================================================================================================")
    println(f"employed people spend $les1%.1f hours a day for leisure,")
    println(f"unemployed people spend $les2%.1f hours a day for leisure,")
  }
}
