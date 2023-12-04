// Databricks notebook source
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object MultiPointInspections_FactTable {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MultiPoint Inspections Master fact table")
      .getOrCreate()

    val spmDealerCodes = Array(1059, 4263, 4536, 6050, 9198, 9226, 12138, 14046, 16064, 19056, 33031, 37066, 39068, 41030, 60451, 60908, 62932, 63136, 63408, 64104, 64233, 5043)
    val formattedDealerCodes = spmDealerCodes.map(code => f"$code%05d")

    val multiPointInspectionsDF = processMultiPointInspections(spark, formattedDealerCodes)
    
    // Display the result DataFrame
    display(multiPointInspectionsDF)
  }

  def processMultiPointInspections(spark: SparkSession, formattedDealerCodes: Array[String]): DataFrame = {
    import spark.implicits._

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
          dealernumber IN (${formattedDealerCodes.mkString(",")})
      """.stripMargin)

    multiPointInspections
  }
}


// COMMAND ----------

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object MultiPointInspections_FactTable {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MultiPoint Inspections Master fact table")
      .getOrCreate()

    val spmDealerCodes = Array(1059, 4263, 4536, 6050, 9198, 9226, 12138, 14046, 16064, 19056, 33031, 37066, 39068, 41030, 60451, 60908, 62932, 63136, 63408, 64104, 64233, 5043)
    val formattedDealerCodes = spmDealerCodes.map(code => f"$code%05d")

    val multiPointInspectionsDF = processMultiPointInspections(spark, formattedDealerCodes)
    
    // Display the result DataFrame
    multiPointInspectionsDF.show()
  }

  def processMultiPointInspections(spark: SparkSession, formattedDealerCodes: Array[String]): DataFrame = {
    import spark.implicits._

    val rawMultiPointInspections = spark.table("tst_service_history_trn.multi_point_inspections")

    val filteredMultiPointInspections = rawMultiPointInspections
      .filter($"dealernumber".isin(formattedDealerCodes: _*))

    val multiPointInspectionsDF = filteredMultiPointInspections
      .select(
        $"date",
        $"dspname",
        substring(concat(lit("0000000"), $"ronumber"), length(concat(lit("0000000"), $"ronumber")) - 6, 7).alias("ronumber"),
        $"vin",
        $"transactionid",
        $"dealernumber",
        $"tires_wheels_lf_sold",
        $"tires_wheels_rf_sold",
        $"tires_wheels_lr_sold",
        $"tires_wheels_rr_sold",
        $"brakes_brakelining_lf_sold",
        $"brakes_brakelining_rf_sold",
        $"brakes_brakelining_lr_sold",
        $"brakes_brakelining_rr_sold",
        $"exterior_washeroperation_sold",
        $"interior_cabinairfilter_sold",
        $"underhood_batteryhealth_sold",
        lit(null).cast(StringType).alias("customerapproveditems"),
        $"mpi_video_viewed",
        $"upsell_amount_at_mpi",
        $"recommended_service_item_count",
        $"recommended_customer_no_response",
        $"recommended_customer_deferred",
        $"recommended_customer_approved",
        $"revieweddigitally",
        $"mpi_awaiting_inspection_end_datetime",
        $"mpi_awaiting_inspection_start_datetime",
        $"mpi_awaiting_repair_end_datetime",
        $"mpi_awaiting_repair_start_datetime",
        $"mpi_being_inspected_end_datetime",
        $"mpi_being_inspected_start_datetime",
        $"mpi_being_repaired_end_datetime",
        $"mpi_being_repaired_start_datetime",
        $"mpi_closed_datetime",
        $"mpi_created_start_datetime".alias("mpi_created_date"),
        $"mpi_created_end_datetime",
        $"mpi_created_start_datetime",
        $"mpi_images_created_datetime",
        $"mpi_images_customer_response_datetime",
        $"mpi_images_sent_datetime",
        $"mpi_images_sent_method",
        $"mpi_pending_approval_end_datetime",
        $"mpi_pending_approval_start_datetime",
        $"mpi_text_open_datetime",
        $"mpi_video_created_datetime",
        $"mpi_video_customer_response_datetime",
        $"mpi_video_sent_datetime",
        $"mpi_video_sent_method",
        $"mpi_with_images",
        $"mpi_with_video",
        $"mpistarttime",
        $"mpicompletedtime"
      )

    multiPointInspectionsDF
  }
}
