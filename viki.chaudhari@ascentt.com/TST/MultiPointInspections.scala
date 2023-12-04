// Databricks notebook source
 
 val spmDealerCodes = Array(1059, 4263, 4536, 6050, 9198, 9226, 12138, 14046, 16064, 19056, 33031, 37066, 39068, 41030, 60451, 60908, 62932, 63136, 63408, 64104, 64233, 5043)
val formattedDealerCodes = spmdealercodes.map(code => f"$code%05d")

val multiPointInspections = spark.sql(
  s"""SELECT DISTINCT date,
      dspname,
      SUBSTRING(CONCAT('0000000', ronumber), LENGTH(CONCAT('0000000', ronumber)) - 6, 7) AS ronumber,
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
      CAST(NULL AS STRING) AS customerapproveditems,
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
      mpi_created_start_datetime AS mpi_created_date,
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
    FROM tst_service_history_trn.multi_point_inspections
    WHERE
      dealernumber IN ($formattedDealerCodes)
  """.stripMargin)

  display(multiPointInspections)

// COMMAND ----------

import org.apache.spark.sql.functions._
//Data cleansing applied on MPI table’s column “customerapproveditems” to remove Tab, Line Break and White spaces.
val cleansedMPI = multiPointInspections.withColumn("customerapproveditems", regexp_replace(org.apache.spark.sql.functions.col("customerapproveditems"), "[\\t\\n\\s]+", ""))

// •	Transformation on below columns-mpi_created_date.
val transformedMPI = cleansedMPI.withColumn("mpi_created_date", to_date(substring(col("mpi_created_date"), 1, 10), "yyyy-MM-dd"))
display(transformedMPI)