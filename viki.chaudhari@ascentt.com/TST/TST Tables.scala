// Databricks notebook source
// DBTITLE 1,Appointments
val appointmentsDF = spark.read.format("redshift")
  .option("url", "jdbc:redshift://<redshift_endpoint>/<database>")
  .option("dbtable", "tst_service_history_trn.appointments")
  .option("user", "<username>")
  .option("password", "<password>")
  .load()

appointmentsDF.createOrReplaceTempView("appointments_Table")

// Define the dealer codes and format them to be 5 digits long
val spmdealercodes = Array(1059, 4263, 4536, 6050, 9198, 9226, 12138, 14046, 16064, 19056, 33031, 37066, 39068, 41030, 60451, 60908, 62932, 63136, 63408, 64104, 64233, 5043)
val formattedDealerCodes = spmdealercodes.map(code => f"$code%05d")

 
// Select required columns and apply transformations
val appointments = spark.sql(s"""
  SELECT DISTINCT dspname, dealernumber, DATE(showeddate) AS showeddate, 
  bookedtime, appointmenttype, vin, transactionid, appointment_source, 
  appointment_ro_amount, DATE(date) AS appointment_created_date, 
  appointment_created_onsite, appointment_loaner, 
  appointment_transportation, confirmed_appointment_method, 
  customer_communication_preferences, scheduler_used, 
  ssc_accepted_at_appointment
  FROM appointments_Table 
  WHERE dealernumber IN (${formattedDealerCodes.map(code => s"'$code'").mkString(",")})
""")

// Display the result
appointments.show()

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
    "scheduler_used", "ssc_accepted_at_appointment", "Showed", "ApptDate"
  )

// Display the result with custom columns
appointmentsWithCustomColumnsDF.show()


// COMMAND ----------

// DBTITLE 1,MobileLaneCheckin
val mobileLaneCheckinsDF = spark.read.format("redshift")
  .option("url", "jdbc:redshift://<redshift_endpoint>/<database>")
  .option("dbtable", "tst_service_history_trn.mobile_lane_checkins")
  .option("user", "<username>")
  .option("password", "<password>")
  .load()

import org.apache.spark.sql.functions._

mobileLaneCheckinsDF.createOrReplaceTempView("mobile_LaneCheck_in_Table")

// Select required columns and apply transformations
val mobileLaneCheckins = spark.sql(s"""
  SELECT DISTINCT dspname, 
  dealernumber AS MLC_dealernumber , 
  RIGHT(CONCAT('00000000', IFNULL(ronumber,'')), 8) as ronumber, 
  appointment, 
  vin AS MLC_vin, -- Rename vin with MLC_ prefix
  date AS MLC_date, -- Rename date with MLC_ prefix
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
  DATE(check_in_completed_datetime) as check_in_date,
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
  FROM mobile_LaneCheck_in_Table 
  WHERE dealernumber IN (${spmdealercodes.mkString(",")})
""")

// Display the result
mobileLaneCheckins.show()


// COMMAND ----------

// Perform full outer join on specified columns
val joinedDF = appointmentsWithCustomColumnsDF.join(
  mobileLaneCheckins,
  (appointmentsWithCustomColumnsDF("vin") === mobileLaneCheckins("MLC_vin")) &&
  (appointmentsWithCustomColumnsDF("dealernumber") === mobileLaneCheckins("MLC_dealernumber")) &&
  (appointmentsWithCustomColumnsDF("showeddate") === mobileLaneCheckins("MLC_date")),
  "outer"
)

// Display the joined DataFrame
joinedDF.show()


// COMMAND ----------

import org.apache.spark.sql.functions._

// Update vin column based on conditions
val updatedVinDF = joinedDF.withColumn("vin", when(col("vin").isNull, col("MLC_vin")).otherwise(col("vin")))

// Update ApptDate column based on conditions
val updatedApptDateDF = updatedVinDF.withColumn("ApptDate", when(col("ApptDate").isNull, col("MLC_date")).otherwise(col("ApptDate")))

// Update showeddate column based on conditions
val finalDF = updatedApptDateDF.withColumn("showeddate", when(col("showeddate").isNull, col("date")).otherwise(col("showeddate")))

// Display the updated DataFrame
finalDF.show()


// COMMAND ----------

// DBTITLE 1,MultiPointInspections

val multiPointInspectionsDF = spark.read.format("redshift")
  .option("url", "jdbc:redshift://<redshift_endpoint>/<database>")
  .option("dbtable", "tst_service_history_trn.multi_point_inspections")
  .option("user", "<username>")
  .option("password", "<password>")
  .load()
import org.apache.spark.sql.functions._

multiPointInspectionsDF.createOrReplaceTempView("multi_Point_Inspections_Table")

// Select distinct columns and apply transformations
val multiPointInspections = spark.sql(s"""
  SELECT DISTINCT
    date,
    dspname,
    RIGHT(CONCAT('00000000', COALESCE(ronumber, '')), 8) as ronumber,
    vin,
    transactionid,
    dealernumber,
    tires_wheels_lf_sold,
    tires_wheels_rf_sold,
    tires_wheels_lr_sold,
    tires_wheels_rr_sold,
    brakes_brakelining_lf_sold,
    brakes_brakelining_rf_sold,
    brakes_brakelining_lr_sold,
    brakes_brakelining_rr_sold,
    exterior_washeroperation_sold,
    interior_cabinairfilter_sold,
    underhood_batteryhealth_sold,
    customerapproveditems,
    mpi_video_viewed,
    upsell_amount_at_mpi,
    recommended_service_item_count,
    recommended_customer_no_response,
    recommended_customer_deferred,
    recommended_customer_approved,
    revieweddigitally,
    mpi_awaiting_inspection_end_datetime,
    mpi_awaiting_inspection_start_datetime,
    mpi_awaiting_repair_end_datetime,
    mpi_awaiting_repair_start_datetime,
    mpi_being_inspected_end_datetime,
    mpi_being_inspected_start_datetime,
    mpi_being_repaired_end_datetime,
    mpi_being_repaired_start_datetime,
    mpi_closed_datetime,
    mpi_created_start_datetime as mpi_created_date,
    mpi_created_end_datetime,
    mpi_created_start_datetime,
    mpi_images_created_datetime,
    mpi_images_customer_response_datetime,
    mpi_images_sent_datetime,
    mpi_images_sent_method,
    mpi_pending_approval_end_datetime,
    mpi_pending_approval_start_datetime,
    mpi_text_open_datetime,
    mpi_video_created_datetime,
    mpi_video_customer_response_datetime,
    mpi_video_sent_datetime,
    mpi_video_sent_method,
    mpi_with_images,
    mpi_with_video,
    mpistarttime,
    mpicompletedtime
  FROM multi_Point_Inspections_Table
  WHERE dealernumber IN (${spmdealercodes.mkString(",")})
""")

// Display the result
multiPointInspections.show()

//Data cleansing applied on MPI table’s column “customerapproveditems” to remove Tab, Line Break and White spaces.
val cleansedMPI = multiPointInspections.withColumn("customerapproveditems", regexp_replace(col("customerapproveditems"), "[\\t\\n\\s]+", ""))

// •	Transformation on below columns-mpi_created_date.
val transformedMPI = cleansedMPI.withColumn("mpi_created_date", to_date(substring(col("mpi_created_date"), 1, 10), "yyyy-MM-dd"))




// COMMAND ----------

//full outer join
val joinedDF = finalDF.join(
  transformedMPI,
  Seq("dealernumber", "ronumber", "vin"),
  "outer"
)

// COMMAND ----------

// DBTITLE 1,DigitalPayment
val digitalPaymentsDF = spark.read.format("redshift")
  .option("url", "jdbc:redshift://<redshift_endpoint>/<database>")
  .option("dbtable", "tst_service_history_trn.digital_payments")
  .option("user", "<username>")
  .option("password", "<password>")
  .load()

// Register the DataFrame as a temporary view
digitalPaymentsDF.createOrReplaceTempView("digital_payments_table")

// Select distinct columns and apply transformations using SQL
val digitalPayments = spark.sql(s"""
  SELECT DISTINCT date, dspname, dealernumber, 
  RIGHT(CONCAT('00000000', COALESCE(ronumber, '')), 8) as ronumber, 
  rodollaramount, vin, transactionid 
  FROM digital_payments_table 
  WHERE dealernumber IN (${spmdealercodes.mkString(",")})
""")

// Display the result
digitalPayments.show()


// COMMAND ----------

//full outer join
val FinalDF = joinedDF.join(
  digitalPayments,
  Seq("dealernumber", "ronumber", "vin"),
  "outer"
)