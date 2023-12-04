// Databricks notebook source
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object TSTMasterFactTable {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TST Master fact table")
      .getOrCreate()

    val appointmentsDF = processAppointments(spark)
    val mobileLaneCheckinsDF = processMobileLaneCheckins(spark, appointmentsDF)
    val finalDF = processDigitalPayments(spark, mobileLaneCheckinsDF)
    
    // You can use the finalDF as needed, for example, writing it to an output location.
    display(finalDF)
  }

  def processAppointments(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val spmDealerCodes = Array(1059, 4263, 4536, 6050, 9198, 9226, 12138, 14046, 16064, 19056, 33031, 37066, 39068, 41030, 60451, 60908, 62932, 63136, 63408, 64104, 64233, 5043)
    val formattedDealerCodes = spmDealerCodes.map(code => f"$code%05d")

    val appointments = spark.sql(
      s"""SELECT DISTINCT dspname,
      dealernumber,
      DATE(showeddate) as showeddate,
      bookedtime,
      appointmenttype,
      vin,
      transactionid,
      appointment_source,
      appointment_ro_amount,
      DATE(date) AS appointment_created_date,
      appointment_created_onsite,
      appointment_loaner,
      appointment_transportation,
      confirmed_appointment_method,
      customer_communication_preferences,
      scheduler_used,
      ssc_accepted_at_appointment,
      recorddate,
      appointment_id,
      SUBSTRING(CONCAT('0000000', ronumber), LENGTH(CONCAT('0000000', ronumber)) - 5, 7) as ronumber,
      ro_close_date,
      ro_status_partner,
      ro_status_dms,
      labor_code,
      dms_appointment_id,
      dms_vin
       FROM tst_service_history_trn.appointments
    WHERE
      dealernumber IN (${formattedDealerCodes.mkString(",")})
  """.stripMargin)


    // Define UDFs
    val showedUDF = udf((showeddate: String) => if (showeddate == null || showeddate == "1900-01-01" || showeddate.trim.isEmpty) "No" else "Yes")
    val apptDateUDF = udf((showed: String, bookedtime: String, showeddate: String) => if (showed == "No") bookedtime else showeddate)

    // Apply transformations
    val appointmentsWithCustomColumnsDF =appointments
  .withColumn("Showed", showedUDF(col("showeddate")))
  .withColumn("ApptDate", apptDateUDF(col("Showed"), col("bookedtime"), col("showeddate")))
  .select(
    "dspname", "dealernumber", "showeddate", "bookedtime", "appointmenttype",
    "vin", "transactionid", "appointment_source", "appointment_ro_amount",
    "appointment_created_date", "appointment_created_onsite", "appointment_loaner",
    "appointment_transportation", "confirmed_appointment_method", "customer_communication_preferences",
    "scheduler_used", "ssc_accepted_at_appointment", "ronumber", "ro_close_date",
    "ro_status_partner", "ro_status_dms", "labor_code", "dms_appointment_id", "dms_vin", 
    "Showed", "ApptDate"
  )
    appointmentsWithCustomColumnsDF
  }

  def processMobileLaneCheckins(spark: SparkSession, appointmentsWithCustomColumnsDF: DataFrame): DataFrame = {
    import spark.implicits._

    val spmDealerCodes = Array(1059, 4263, 4536, 6050, 9198, 9226, 12138, 14046, 16064, 19056, 33031, 37066, 39068, 41030, 60451, 60908, 62932, 63136, 63408, 64104, 64233, 5043)
    val formattedDealerCodes = spmDealerCodes.map(code => f"$code%05d")

    val mobileLaneCheckins = spark.sql(

  s"""SELECT DISTINCT dspname AS MLC_dspname,

      dealernumber AS MLC_dealernumber,

      SUBSTRING(CONCAT('0000000', ronumber), LENGTH(CONCAT('0000000', ronumber)) - 6, 7) AS ronumber,

      appointment,

      vin AS MLC_vin,

      date,

      rodollaramount,

      promisetime,

      waiter,

      dropoff,

      openrecalls,

      expressmaintenance,

      transactionid,

      inspection_at_checkin_completed,

      upsell_amount_at_checkin,

      check_in_completed_datetime,

      DATE(check_in_completed_datetime) AS check_in_date,

      check_in_images_sent,

      check_in_images_sent_method,

      check_in_response_datetime,

      check_in_text_open_datetime,

      check_in_video_sent,

      check_in_video_sent_method,

      check_in_with_images,

      check_in_with_video,

      declined_service_sold,

      loaner_requested_at_check_in,

      ssc_accepted_at_check_in,

      ssc_notified_at_check_in,

      transportation_requested_at_check_in

    FROM tst_service_history_trn.mobile_lane_checkins

    WHERE
      dealernumber IN ($formattedDealerCodes) 

  """.stripMargin)

    // Perform full outer join
    val joinedDF = appointmentsWithCustomColumnsDF.join(
      mobileLaneCheckins,
      (appointmentsWithCustomColumnsDF("vin") === mobileLaneCheckins("MLC_vin")) &&
        (appointmentsWithCustomColumnsDF("dealernumber") === mobileLaneCheckins("MLC_dealernumber")) &&
        (appointmentsWithCustomColumnsDF("showeddate") === mobileLaneCheckins("date")),
      "outer"
    )
    // Update vin column based on conditions
    val updatedVinDF = joinedDF.withColumn("vin", when(col("vin").isNull, col("MLC_vin")).otherwise(col("vin")))

	// Update ApptDate column based on conditions
    val updatedApptDateDF = updatedVinDF.withColumn("ApptDate", when(col("ApptDate").isNull, 	col("date")).otherwise(col("ApptDate")))

	// Update showeddate column based on conditions
    val finalDF = updatedApptDateDF.withColumn("showeddate", when(col("showeddate").isNull, 	col("date")).otherwise(col("showeddate")))

    finalDF
  }

  def processDigitalPayments(spark: SparkSession, finalDF: DataFrame): DataFrame = {
    import spark.implicits._

    val spmDealerCodes = Array(1059, 4263, 4536, 6050, 9198, 9226, 12138, 14046, 16064, 19056, 33031, 37066, 39068, 41030, 60451, 60908, 62932, 63136, 63408, 64104, 64233, 5043)
    val formattedDealerCodes = spmDealerCodes.map(code => f"$code%05d")

    val digitalPayments = spark.sql(
  s"""
    SELECT
      DISTINCT date,
      dspname,
      dealernumber,
      SUBSTRING(CONCAT('0000000', ronumber), LENGTH(CONCAT('0000000', ronumber)) - 6, 7) AS ronumber,
      rodollaramount,
      vin,
      transactionid
    FROM
      tst_service_history_trn.digital_payments
    WHERE
      dealernumber IN (${formattedDealerCodes.mkString(",")})
    """
	)


    // Perform full outer join
    val finalDF2 = finalDF.join(
      digitalPayments,
      Seq("dealernumber", "ronumber", "vin"),
      "outer"
    )
    finalDF2
  }
}


// COMMAND ----------

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object TSTMasterFactTable {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TST Master fact table")
      .getOrCreate()

    val appointmentsDF = processAppointments(spark)
    val mobileLaneCheckinsDF = processMobileLaneCheckins(spark, appointmentsDF)
    val finalDF = processDigitalPayments(spark, mobileLaneCheckinsDF)
    
    // You can use the finalDF as needed, for example, writing it to an output location.
    finalDF.show()
  }

  def processAppointments(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val spmDealerCodes = Array(1059, 4263, 4536, 6050, 9198, 9226, 12138, 14046, 16064, 19056, 33031, 37066, 39068, 41030, 60451, 60908, 62932, 63136, 63408, 64104, 64233, 5043)
    val formattedDealerCodes = spmDealerCodes.map(code => f"$code%05d")

    val appointments = spark.read.table("tst_service_history_trn.appointments")
      .filter($"dealernumber".isin(spmDealerCodes: _*))
      .select(
        $"dspname", $"dealernumber", $"showeddate".cast("date"), $"bookedtime",
        $"appointmenttype", $"vin", $"transactionid", $"appointment_source",
        $"appointment_ro_amount", $"date".cast("date").as("appointment_created_date"),
        $"appointment_created_onsite", $"appointment_loaner", $"appointment_transportation",
        $"confirmed_appointment_method", $"customer_communication_preferences", $"scheduler_used",
        $"ssc_accepted_at_appointment", $"recorddate", $"appointment_id",
        expr(s"SUBSTRING(CONCAT('0000000', ronumber), LENGTH(CONCAT('0000000', ronumber)) - 5, 7)").as("ronumber"),
        $"ro_close_date", $"ro_status_partner", $"ro_status_dms", $"labor_code", $"dms_appointment_id", $"dms_vin"
      )

    // Define UDFs
    val showedUDF = udf((showeddate: String) => if (showeddate == null || showeddate == "1900-01-01" || showeddate.trim.isEmpty) "No" else "Yes")
    val apptDateUDF = udf((showed: String, bookedtime: String, showeddate: String) => if (showed == "No") bookedtime else showeddate)

    // Apply transformations
    val appointmentsWithCustomColumnsDF = appointments
      .withColumn("Showed", showedUDF($"showeddate"))
      .withColumn("ApptDate", apptDateUDF($"Showed", $"bookedtime", $"showeddate"))
      .select(
        "dspname", "dealernumber", "showeddate", "bookedtime", "appointmenttype",
        "vin", "transactionid", "appointment_source", "appointment_ro_amount",
        "appointment_created_date", "appointment_created_onsite", "appointment_loaner",
        "appointment_transportation", "confirmed_appointment_method", "customer_communication_preferences",
        "scheduler_used", "ssc_accepted_at_appointment", "ronumber", "ro_close_date",
        "ro_status_partner", "ro_status_dms", "labor_code", "dms_appointment_id", "dms_vin",
        "Showed", "ApptDate"
      )

    appointmentsWithCustomColumnsDF
  }

  def processMobileLaneCheckins(spark: SparkSession, appointmentsWithCustomColumnsDF: DataFrame): DataFrame = {
    import spark.implicits._

    val spmDealerCodes = Array(1059, 4263, 4536, 6050, 9198, 9226, 12138, 14046, 16064, 19056, 33031, 37066, 39068, 41030, 60451, 60908, 62932, 63136, 63408, 64104, 64233, 5043)
    val formattedDealerCodes = spmDealerCodes.map(code => f"$code%05d")

    val mobileLaneCheckins = spark.read.table("tst_service_history_trn.mobile_lane_checkins")
      .filter($"dealernumber".isin(formattedDealerCodes: _*))
      .select(
        $"dspname".as("MLC_dspname"), $"dealernumber".as("MLC_dealernumber"),
        expr(s"SUBSTRING(CONCAT('0000000', ronumber), LENGTH(CONCAT('0000000', ronumber)) - 6, 7)").as("ronumber"),
        $"appointment", $"vin".as("MLC_vin"), $"date",
        $"rodollaramount", $"promisetime", $"waiter", $"dropoff",
        $"openrecalls", $"expressmaintenance", $"transactionid",
        $"inspection_at_checkin_completed", $"upsell_amount_at_checkin",
        $"check_in_completed_datetime",
        expr("CAST(check_in_completed_datetime AS date)").as("check_in_date"),
        $"check_in_images_sent", $"check_in_images_sent_method",
        $"check_in_response_datetime", $"check_in_text_open_datetime",
        $"check_in_video_sent", $"check_in_video_sent_method",
        $"check_in_with_images", $"check_in_with_video", $"declined_service_sold",
        $"loaner_requested_at_check_in", $"ssc_accepted_at_check_in",
        $"ssc_notified_at_check_in", $"transportation_requested_at_check_in"
      )

    // Perform full outer join
    val joinedDF = appointmentsWithCustomColumnsDF.join(
      mobileLaneCheckins,
      (appointmentsWithCustomColumnsDF("vin") === mobileLaneCheckins("MLC_vin")) &&
        (appointmentsWithCustomColumnsDF("dealernumber") === mobileLaneCheckins("MLC_dealernumber")) &&
        (appointmentsWithCustomColumnsDF("showeddate") === mobileLaneCheckins("date")),
      "outer"
    )

    // Update vin column based on conditions
    val updatedVinDF = joinedDF.withColumn("vin", when($"vin".isNull, $"MLC_vin").otherwise($"vin"))

    // Update ApptDate column based on conditions
    val updatedApptDateDF = updatedVinDF.withColumn("ApptDate", when($"ApptDate".isNull, $"date").otherwise($"ApptDate"))

    // Update showeddate column based on conditions
    val finalDF = updatedApptDateDF.withColumn("showeddate", when($"showeddate".isNull, $"date").otherwise($"showeddate"))

    finalDF
  }

  def processDigitalPayments(spark: SparkSession, finalDF: DataFrame): DataFrame = {
    import spark.implicits._

    val spmDealerCodes = Array(1059, 4263, 4536, 6050, 9198, 9226, 12138, 14046, 16064, 19056, 33031, 37066, 39068, 41030, 60451, 60908, 62932, 63136, 63408, 64104, 64233, 5043)
    val formattedDealerCodes = spmDealerCodes.map(code => f"$code%05d")

    val digitalPayments = spark.read.table("tst_service_history_trn.digital_payments")
      .select(
        $"date",
        $"dspname",
        $"dealernumber",
        expr(s"SUBSTRING(CONCAT('0000000', ronumber), LENGTH(CONCAT('0000000', ronumber)) - 6, 7)").as("ronumber"),
        $"rodollaramount",
        $"vin",
        $"transactionid"
      )
    .filter($"dealernumber".isin(formattedDealerCodes: _*))k

  // Perform full outer join
    val finalDF2 = finalDF.join(
      digitalPayments,
      Seq("dealernumber", "ronumber", "vin"),
      "outer"
    )
    finalDF2
  }
}


// COMMAND ----------

