// Databricks notebook source
// DBTITLE 1,Appointment
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_date, date_sub}

val spark = SparkSession.builder()
  .appName("TST Master fact table")
  .getOrCreate()

// Define the dealer codes and format them to be 5 digits long
val spmdealercodes = Array(1059, 4263, 4536, 6050, 9198, 9226, 12138, 14046, 16064, 19056, 33031, 37066, 39068, 41030, 60451, 60908, 62932, 63136, 63408, 64104, 64233, 5043)
val formattedDealerCodes = spmdealercodes.map(code => f"$code%05d")
val lastYearDate = date_sub(current_date(), 365) // Subtracts 365 days from the current date

 
// Select required columns and apply transformations
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
      DATE(recorddate) >= '$lastYearDate'
      AND
      dealernumber IN (${formattedDealerCodes.mkString(",")})
  """.stripMargin)

// Display the result
display(appointments)

// COMMAND ----------

import org.apache.spark.sql.functions._

// Define the UDFs (User-Defined Functions) for the custom columns
val showedUDF = udf((showeddate: String) => if (showeddate == null || showeddate == "1900-01-01" || showeddate.trim.isEmpty) "No" else "Yes")
val apptDateUDF = udf((showed: String, bookedtime: String, showeddate: String) => if (showed == "No") bookedtime else showeddate)

// Select required columns and apply transformations including custom columns
val appointmentsWithCustomColumnsDF = appointments
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

// Display the result with custom columns
display(appointmentsWithCustomColumnsDF)



// COMMAND ----------

// DBTITLE 1,MobileLaneCheckin
val formattedDealerCodes = spmdealercodes.map(code => f"$code%05d")
val lastYearDate = date_sub(current_date(), 365) // Subtracts 365 days from the current date

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
      DATE(recorddate) >= '$lastYearDate'
      AND
      dealernumber IN ($formattedDealerCodes) 

  """.stripMargin)
  display(mobileLaneCheckins)


// COMMAND ----------

// Perform full outer join on specified columns
val joinedDF = appointmentsWithCustomColumnsDF.join(
  mobileLaneCheckins,
  (appointmentsWithCustomColumnsDF("vin") === mobileLaneCheckins("MLC_vin")) &&
  (appointmentsWithCustomColumnsDF("dealernumber") === mobileLaneCheckins("MLC_dealernumber")) &&
  (appointmentsWithCustomColumnsDF("showeddate") === mobileLaneCheckins("date")),
  "outer"
)

// Display the joined DataFrame
display(joinedDF)


// COMMAND ----------

import org.apache.spark.sql.functions._

// Update vin column based on conditions
val updatedVinDF = joinedDF.withColumn("vin", when(col("vin").isNull, col("MLC_vin")).otherwise(col("vin")))

// Update ApptDate column based on conditions
val updatedApptDateDF = updatedVinDF.withColumn("ApptDate", when(col("ApptDate").isNull, col("date")).otherwise(col("ApptDate")))

// Update showeddate column based on conditions
val finalDF = updatedApptDateDF.withColumn("showeddate", when(col("showeddate").isNull, col("date")).otherwise(col("showeddate")))

// Display the updated DataFrame
display(finalDF1)


// COMMAND ----------

// DBTITLE 1,DigitalPayment
val formattedDealerCodes = spmdealercodes.map(code => f"$code%05d")
val lastYearDate = date_sub(current_date(), 365) // Subtracts 365 days from the current date

val digitalPayments =
spark.sql(
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
      DATE(recorddate) >= '$lastYearDate'
      AND
      dealernumber IN (${formattedDealerCodes.mkString(",")})
  """
)
// Display the result
display(digitalPayments)


// COMMAND ----------

//full outer join
val FinalDF2 = FinalDF.join(
  digitalPayments,
  Seq("dealernumber", "ronumber", "vin"),
  "outer"
)
display(FinalDF2)

// COMMAND ----------

