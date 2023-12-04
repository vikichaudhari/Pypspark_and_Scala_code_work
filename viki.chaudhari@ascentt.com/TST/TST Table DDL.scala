// Databricks notebook source

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("TST Master fact table")
  .getOrCreate()


// COMMAND ----------

// DBTITLE 1,DimPaymentType DDL
val createDimPaymentTypeTableDDL =
  """
    CREATE TABLE dimPaymentType (
      PayTypeKey INT PRIMARY KEY,
      PayType NVARCHAR(50),
      SortOrder NVARCHAR(50),
      RowChangeReason NVARCHAR(200),
      InsertTimestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UpdateTimestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  """

spark.sql(createDimPaymentTypeTableDDL)


// COMMAND ----------

// DBTITLE 1,DimRO DDL

val createDimRepairOrderTableDDL =
  """
    CREATE TABLE dimRepairOrder (
      RepairOrderKey INT PRIMARY KEY,
      RepairOrder NVARCHAR(10),
      SortOrder NVARCHAR(50),
      RowChangeReason NVARCHAR(200),
      InsertTimestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UpdateTimestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  """

spark.sql(createDimRepairOrderTableDDL)


// COMMAND ----------

// DBTITLE 1,TST Master Fact Table DDL
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("TST Master fact table")
  .getOrCreate()

val TSTMasterFactTableDDL =
  """
    CREATE TABLE fact_table (
      PayTypeKey INT,
      RepairOrderKey INT,
      InsertAuditKey INT,
      UpdateAuditKey INT,
      dspname STRING,
      dealernumber STRING,
      bookedtime STRING,
      appointmenttype STRING,
      vin STRING,
      APT_transactionid STRING,
      appointment_source STRING,
      appointment_ro_amount STRING,
      appointment_created_date STRING,
      appointment_created_onsite STRING,
      appointment_loaner STRING,
      appointment_transportation STRING,
      confirmed_appointment_method STRING,
      customer_communication_preferences STRING,
      scheduler_used STRING,
      ssc_accepted_at_appointment STRING,
      APT_recorddate STRING,
      APT_appointment_id STRING,
      ronumber STRING,
      APT_ro_close_date STRING,
      APT_ro_status_partner STRING,
      APT_ro_status_dms STRING,
      APT_labor_code STRING,
      APT_dms_appointment_id STRING,
      APT_dms_vin STRING,
      showeddate STRING,
      Showed STRING,
      ApptDate STRING,
      waiter STRING,
      upsell_amount_at_checkin STRING,
      transportation_requested_at_check_in STRING,
      ssc_notified_at_check_in STRING,
      ssc_accepted_at_check_in STRING,
      MLC_rodollaramount STRING,
      promisetime STRING,
      openrecalls STRING,
      MLC_transactionid STRING,
      loaner_requested_at_check_in STRING,
      inspection_at_checkin_completed STRING,
      expressmaintenance STRING,
      dropoff STRING,
      declined_service_sold STRING,
      check_in_with_video STRING,
      check_in_with_images STRING,
      check_in_video_sent_method STRING,
      check_in_video_sent STRING,
      check_in_text_open_datetime STRING,
      check_in_response_datetime STRING,
      check_in_images_sent_method STRING,
      check_in_images_sent STRING,
      check_in_date STRING,
      check_in_completed_datetime STRING,
      appointment STRING,
      DP_date STRING,
      DP_rodollaramount STRING,
      DP_transactionid STRING,
      MPI_Date STRING,
      MPI_TransactionID STRING,
      tires_wheels_lf_sold STRING,
      tires_wheels_rf_sold STRING,
      tires_wheels_lr_sold STRING,
      tires_wheels_rr_sold STRING,
      brakes_brakelining_lf_sold STRING,
      brakes_brakelining_rf_sold STRING,
      brakes_brakelining_lr_sold STRING,
      brakes_brakelining_rr_sold STRING,
      exterior_washeroperation_sold STRING,
      interior_cabinairfilter_sold STRING,
      underhood_batteryhealth_sold STRING,
      mpi_video_viewed STRING,
      upsell_amount_at_mpi STRING,
      recommended_service_item_count STRING,
      recommended_customer_no_response STRING,
      recommended_customer_deferred STRING,
      recommended_customer_approved STRING,
      revieweddigitally STRING,
      mpi_awaiting_inspection_end_datetime STRING,
      mpi_awaiting_inspection_start_datetime STRING,
      mpi_awaiting_repair_end_datetime STRING,
      mpi_awaiting_repair_start_datetime STRING,
      mpi_being_inspected_end_datetime STRING,
      mpi_being_inspected_start_datetime STRING,
      mpi_being_repaired_end_datetime STRING,
      mpi_being_repaired_start_datetime STRING,
      mpi_closed_datetime STRING,
      mpi_created_date STRING,
      mpi_created_end_datetime STRING,
      mpi_created_start_datetime STRING,
      mpi_images_created_datetime STRING,
      mpi_images_customer_response_datetime STRING,
      mpi_images_sent_datetime STRING,
      mpi_images_sent_method STRING,
      mpi_pending_approval_end_datetime STRING,
      mpi_pending_approval_start_datetime STRING,
      mpi_text_open_datetime STRING,
      mpi_video_created_datetime STRING,
      mpi_video_customer_response_datetime STRING,
      mpi_video_sent_datetime STRING,
      mpi_video_sent_method STRING,
      mpi_with_images STRING,
      mpi_with_video STRING,
      mpistarttime STRING,
      mpicompletedtime STRING,
      customerapproveditems STRING,
      InsertTimestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UpdateTimestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      FOREIGN KEY (PayTypeKey) REFERENCES dimPaymentType(PayTypeKey),
      FOREIGN KEY (RepairOrderKey) REFERENCES dimRepairOrder(RepairOrderKey)
    )
  """

spark.sql(TSTMasterFactTableDDL)


// COMMAND ----------


// Use SQL JOIN to populate PayTypeKey and RepairOrderKey
val populatedFactTableDF = spark.sql(
  """
    SELECT
      ft.*,
      dp.PayTypeKey,
      dr.RepairOrderKey
    FROM
      TSTMasterFactTableDDL ft
    LEFT JOIN
      dimPaymentType dp
    ON
      ft.PayTypeKey = dp.PayTypeKey
    LEFT JOIN
      dimRepairOrder dr
    ON
      ft.RepairOrderKey = dr.RepairOrderKey
  """
)

// Temporary table to hold the populated fact table data
populatedFactTableDF.createOrReplaceTempView("populated_fact_table")

// Insert data into the fact_table with keys populated
spark.sql("INSERT INTO TABLE TSTMasterFactTableDDL SELECT * FROM populated_fact_table")
