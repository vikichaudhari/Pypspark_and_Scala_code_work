// Databricks notebook source
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{col, expr, date_format, to_date}

object NSH_ECSC_MasterFactTable {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NSH_ECSC Master fact table")
      .getOrCreate()

    import spark.implicits._

    val spmDealerCodes = Array(1059, 4263, 4536, 6050, 9198, 9226, 12138, 14046, 16064, 19056, 33031, 37066, 39068, 41030, 60451, 60908, 62932, 63136, 63408, 64104, 64233, 5043)
    val formattedDealerCodes = spmDealerCodes.map(code => f"$code%05d")

    // First set of DataFrame operations
    val toyotaData = readData("nsh_trn.tshro", formattedDealerCodes, spark)
    val lexusData = readData("nsh_trn.lshro", formattedDealerCodes, spark)
    val result1 = toyotaData.union(lexusData)

    // Second set of DataFrame operations
    val toyotaData2 = readData2("nsh_trn.tshcond", formattedDealerCodes, spark)
    val lexusData2 = readData2("nsh_trn.lshcond", formattedDealerCodes, spark)
    val result2 = toyotaData2.union(lexusData2)

    // Third set of DataFrame operations
    val toyotaSurveyData = readData3("ecsc_trn.toyota_service_survey_resp_us", formattedDealerCodes, spark)
    val lexusSurveyData = readData3("ecsc_trn.lexus_service_survey_resp_us", formattedDealerCodes, spark)
    val result3 = toyotaSurveyData.union(lexusSurveyData)

    // Joining results
    val joinedResult = result1.join(result2, Seq("rdlrno", "rdte", "ronumber"),"outer")
    val allColumns = joinedResult.columns
    val uniqueColumns = allColumns.map(column => col(column).as(column.replaceAll("\\.", "_")))
    val finalResult = joinedResult.select(uniqueColumns: _*)

    // Rename the columns in result3 to match the corresponding columns in finalResult
    val finalResultRenamed = finalResult
      .withColumnRenamed("rdlrno", "service_dealer_code")
      .withColumnRenamed("rdte", "service_repair_order_date")
      .withColumnRenamed("ronumber", "service_repair_order_number")
      .withColumnRenamed("vvin", "vin")
      .withColumn("service_repair_order_date", date_format(to_date(col("service_repair_order_date"), "MM/dd/yyyy"), "MM-dd-yyyy"))

    // Perform the join without renaming columns
    val joinColumns = Seq("service_dealer_code", "service_repair_order_date", "service_repair_order_number", "vin")
    val joinedDF = result3.join(finalResultRenamed, joinColumns, "outer")

    // Display the resulting DataFrame
    joinedDF.show()

    // Optionally, you can also display the DataFrame using display function if you are using a notebook environment
    // display(joinedDF)

    spark.stop()
  }

  def readData(tableName: String, dealerCodes: Array[String], spark: SparkSession): DataFrame = {
    spark.read.table(tableName)
      .filter(col("rdlrno").isin(dealerCodes: _*))
      .filter(expr("SUBSTRING(rdte, -2) IN ('21', '22', '20', '23')"))
      .select("rdlrno", "rdte", "rorderno", "vvin")
      .withColumn("ronumber", expr("RIGHT(CONCAT('00000000', COALESCE(rorderno, '')), 8)"))
  }

  def readData2(tableName: String, dealerCodes: Array[String], spark: SparkSession): DataFrame = {
    spark.read.table(tableName)
      .filter(col("rdlrno").isin(dealerCodes: _*))
      .filter(expr("SUBSTRING(rdte, -2) IN ('21', '22', '20', '23')"))
      .groupBy("rlbrcd", "rdlrno", "rorderno", "rdte")
      .agg(
        expr("COUNT(rlbrcd) as TotalConditions"),
        expr("SUM(CAST(rtcond AS FLOAT)) as TotalDollarAmount"),
        expr("SUM(CAST(rthours AS FLOAT)) as LaborHours"),
        expr("SUM(CAST(rtlabor AS FLOAT)) as LaborCharges"),
        expr("SUM(CAST(rtprts AS FLOAT)) as PartsCharges")
      )
      .withColumn("ronumber", expr("RIGHT(CONCAT('00000000', COALESCE(rorderno, '')), 8)"))
  }

  def readData3(tableName: String, dealerCodes: Array[String], spark: SparkSession): DataFrame = {
    spark.read.table(tableName)
      .filter(col("service_dealer_code").isin(dealerCodes: _*))
      .select("service_dealer_code", "vin", "response_overall_service_experience_rating", "service_repair_order_number", "service_repair_order_date", "vehicle_make")
      .withColumn("ronumber", expr("RIGHT(CONCAT('00000000', COALESCE(service_repair_order_number, '')), 8)"))
  }
}
