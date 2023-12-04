// Databricks notebook source
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("TST Master fact table")
  .getOrCreate()

// Define the dealer codes and format them to be 5 digits long
val spmdealercodes = Array(1059, 4263, 4536, 6050, 9198, 9226, 12138, 14046, 16064, 19056, 33031, 37066, 39068, 41030, 60451, 60908, 62932, 63136, 63408, 64104, 64233, 5043)
val formattedDealerCodes = spmdealercodes.map(code => f"$code%05d")


val toyotaroData = spark.sql(
  s"""SELECT rdlrno,
      RIGHT(CONCAT('00000000', COALESCE(rorderno,'')), 8) as ronumber,
      rdte,
      vvin
    FROM nsh_trn.tshro
    WHERE (SUBSTRING(rdte, -2) = '21' OR SUBSTRING(rdte, -2) = '22' OR SUBSTRING(rdte, -2) = '20' OR SUBSTRING(rdte, -2) = '23')
    AND rdlrno IN (${formattedDealerCodes.mkString(",")})
  """
)

val lexusroData = spark.sql(
  s"""SELECT rdlrno,
      RIGHT(CONCAT('00000000', COALESCE(rorderno,'')), 8) as ronumber,
      rdte,
      vvin
    FROM nsh_trn.lshro
    WHERE (SUBSTRING(rdte, -2) = '21' OR SUBSTRING(rdte, -2) = '22' OR SUBSTRING(rdte, -2) = '20' OR SUBSTRING(rdte, -2) = '23')
    AND rdlrno IN (${formattedDealerCodes.mkString(",")})
  """
)

val result1 = toyotaroData.union(lexusroData)
display(result1)


// COMMAND ----------

val spmdealercodes = Array(1059, 4263, 4536, 6050, 9198, 9226, 12138, 14046, 16064, 19056, 33031, 37066, 39068, 41030, 60451, 60908, 62932, 63136, 63408, 64104, 64233, 5043)
val formattedDealerCodes = spmdealercodes.map(code => f"$code%05d")


val toyotacondData = spark.sql(
  s"""SELECT rdlrno,
      rdte,
      RIGHT(CONCAT('00000000', COALESCE(rorderno,'')), 8) as ronumber,
      rlbrcd,
      COUNT(rlbrcd) as TotalConditions,
      SUM(CAST(rtcond AS FLOAT)) as TotalDollarAmount,
      SUM(CAST(rthours AS FLOAT)) as LaborHours,
      SUM(CAST(rtlabor AS FLOAT)) as LaborCharges,
      SUM(CAST(rtprts AS FLOAT)) as PartsCharges
    FROM nsh_trn.tshcond
    WHERE (SUBSTRING(rdte, -2) = '21' OR SUBSTRING(rdte, -2) = '22' OR SUBSTRING(rdte, -2) = '20' OR SUBSTRING(rdte, -2) = '23')
    AND rdlrno IN (${formattedDealerCodes.mkString(",")})
    GROUP BY rlbrcd, rdlrno, rorderno, rdte
  """
)

val lexuscondData = spark.sql(
  s"""SELECT rdlrno,
      rdte,
      RIGHT(CONCAT('00000000', COALESCE(rorderno,'')), 8) as ronumber,
      rlbrcd,
      COUNT(rlbrcd) as TotalConditions,
      SUM(CAST(rtcond AS FLOAT)) as TotalDollarAmount,
      SUM(CAST(rthours AS FLOAT)) as LaborHours,
      SUM(CAST(rtlabor AS FLOAT)) as LaborCharges,
      SUM(CAST(rtprts AS FLOAT)) as PartsCharges
    FROM nsh_trn.lshcond
    WHERE (SUBSTRING(rdte, -2) = '21' OR SUBSTRING(rdte, -2) = '22' OR SUBSTRING(rdte, -2) = '20' OR SUBSTRING(rdte, -2) = '23')
    AND rdlrno IN (${formattedDealerCodes.mkString(",")})
    GROUP BY rlbrcd, rdlrno, rorderno, rdte
  """
)

val result2 = toyotacondData.union(lexuscondData)
display(result2)


// COMMAND ----------

import org.apache.spark.sql.functions._

  val joinedResult = result1.join(result2, Seq("rdlrno", "rdte", "ronumber"))

val allColumns = joinedResult.columns
val uniqueColumns = allColumns.map(column => s"$column as ${column.replaceAll("\\.", "_")}")

val finalResult = joinedResult.selectExpr(uniqueColumns: _*)

// Rename the columns in result3 to match the corresponding columns in finalResult
    val finalResultRenamed = finalResult.withColumnRenamed("rdlrno", "service_dealer_code")
    .withColumnRenamed("rdte", "service_repair_order_date")
    .withColumnRenamed("ronumber", "service_repair_order_number")
    .withColumnRenamed("vvin", "vin")
val resultDF = finalResultRenamed.withColumn("service_repair_order_date", date_format(to_date(col("service_repair_order_date"), "MM/dd/yyyy"), "MM-dd-yyyy"))
display(resultDF)


// COMMAND ----------

val spmdealercodes = Array(1059, 4263, 4536, 6050, 9198, 9226, 12138, 14046, 16064, 19056, 33031, 37066, 39068, 41030, 60451, 60908, 62932, 63136, 63408, 64104, 64233, 5043)
val formattedDealerCodes = spmdealercodes.map(code => f"$code%05d")

val toyotaSurveyData = spark.sql(
  s"""SELECT service_dealer_code,
      vin,
      response_overall_service_experience_rating,
      RIGHT(CONCAT('00000000', COALESCE(service_repair_order_number,'')), 8) as service_repair_order_number,
      service_repair_order_date,
      vehicle_make
    FROM ecsc_trn.toyota_service_survey_resp_us
    WHERE service_dealer_code IN (${formattedDealerCodes.mkString(",")})
  """
)

val lexusSurveyData = spark.sql(
  s"""SELECT service_dealer_code,
      vin,
      response_overall_service_experience_rating,
      RIGHT(CONCAT('00000000', COALESCE(service_repair_order_number,'')), 8) as service_repair_order_number,
      service_repair_order_date,
      vehicle_make
    FROM ecsc_trn.lexus_service_survey_resp_us
    WHERE service_dealer_code IN (${formattedDealerCodes.mkString(",")})
  """
)

val result3 = toyotaSurveyData.union(lexusSurveyData)
display(result3)


// COMMAND ----------

val joinColumns = Seq("service_dealer_code", "service_repair_order_date", "service_repair_order_number", "vin")
// Perform the join without renaming columns
val joinedDF = result3.join(resultDF, joinColumns, "outer")
 
// Display the resulting DataFrame
display(joinedDF)

// COMMAND ----------

