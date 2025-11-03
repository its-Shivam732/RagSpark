package com.rag

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import io.delta.tables._
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper
import java.net.URI
import java.io.InputStream

object RAGDocNormalized {

  def extractTextFromPDF(uri: String): String = {
    var doc: PDDocument = null
    var in: InputStream = null
    try {
      // Handle both s3a:// and file:// URIs
      // Spark's internal FileSystem will handle S3 reading for us via binaryFiles
      // But for direct URL reading, we need to use Hadoop FileSystem
      val inputUri = new URI(uri)

      if (uri.startsWith("s3a://") || uri.startsWith("s3://")) {
        // For S3, we'll let Spark handle it via the FileSystem
        // This is a placeholder - actual S3 reading will be done by Spark
        val conf = new org.apache.hadoop.conf.Configuration()
        val fs = org.apache.hadoop.fs.FileSystem.get(inputUri, conf)
        val path = new org.apache.hadoop.fs.Path(uri)
        in = fs.open(path)
      } else {
        // For file:// or http://
        in = new java.net.URL(uri).openStream()
      }

      doc = PDDocument.load(in)
      val stripper = new PDFTextStripper()
      val raw = stripper.getText(doc)
      raw.replaceAll("\\s+", " ").trim
    } catch {
      case e: Exception =>
        println(s"[WARN] Failed to read $uri : ${e.getMessage}")
        ""
    } finally {
      if (doc != null) try { doc.close() } catch { case _: Throwable => }
      if (in != null) try { in.close() } catch { case _: Throwable => }
    }
  }

  def main(args: Array[String]): Unit = {
    val runTimestamp = java.time.LocalDateTime.now()
    // Add a few blank lines before each new pipeline run
    println("\n\n")
    AuditLogger.audit("=" * 80)
    AuditLogger.audit(s"🚀 NEW PIPELINE RUN STARTING AT: $runTimestamp")
    AuditLogger.audit("=" * 80)
    AuditLogger.audit("STEP 1: Document Normalization")
    AuditLogger.audit("-" * 80)

    val spark = SparkSession.builder()
      .appName("RAGDocNormalized")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      // Only set master for local mode
      .config("spark.master", if (Config.isEMR) "yarn" else "local[*]")
      .getOrCreate()

    import spark.implicits._

    try {
      if (args.length < 1) {
        System.err.println("Usage: RAGDocNormalized <pdfList.txt>")
        System.exit(1)
      }
      val listPath = args(0)
      val deltaPath = Config.docNormalizedPath  // Use Config path

//      AuditLogger.audit(s"Input PDF list: $listPath")
//      AuditLogger.audit(s"Output Delta path: $deltaPath")

      // Read PDF list
      val pdfPathsDF = spark.read.text(listPath).repartition(10).toDF("uri")
      val totalPdfs = pdfPathsDF.count()
      AuditLogger.audit(s"Total PDFs in input list: $totalPdfs")

      // Register UDF
      val extractTextUDF = udf(extractTextFromPDF _)

      // Check existing documents
      val existingDocs = if (DeltaTable.isDeltaTable(spark, deltaPath)) {
        val existing = spark.read.format("delta").load(deltaPath)
          .select("docId", "contentHash", "uri").distinct()
        val count = existing.count()
        AuditLogger.audit(s"Found existing Delta table with $count documents")
        existing
      } else {
        AuditLogger.audit("No existing Delta table found - this is the first run")
        spark.emptyDataFrame
      }

      // Normalize documents
      val normalizedDocs = pdfPathsDF
        .withColumn("text", extractTextUDF(col("uri")))
        .filter(length(col("text")) > 0)
        .withColumn("language", lit("en"))
        .withColumn("title", regexp_extract(col("text"), "^(.{1,80})", 1))
        .withColumn("docId", sha2(col("uri"), 256))
        .withColumn("contentHash", sha2(col("text"), 256))
        .withColumn("version_ts", current_timestamp())

      val successfulExtractions = normalizedDocs.count()
      val failedExtractions = totalPdfs - successfulExtractions
      AuditLogger.audit(s"PDF extraction: $successfulExtractions successful, $failedExtractions failed")

      // Determine new/changed documents
      val toUpsert = if (existingDocs.isEmpty) {
        AuditLogger.audit("All documents are NEW (first run)")
        normalizedDocs
      } else {
        val newOrChanged = normalizedDocs.join(
          existingDocs,
          Seq("docId", "contentHash"),
          "left_anti"
        )
        val count = newOrChanged.count()

        if (count == 0) {
          AuditLogger.audit("✅ NO NEW OR CHANGED DOCUMENTS - All documents are up to date")
        } else {
          AuditLogger.audit(s"Found $count NEW or CHANGED documents (out of $successfulExtractions total)")

          // Log which documents are new vs changed
          val existingDocIds = existingDocs.select("docId").distinct()
          val newDocs = newOrChanged.join(existingDocIds, Seq("docId"), "left_anti")
          val changedDocs = newOrChanged.join(existingDocIds, Seq("docId"), "left_semi")

          val newCount = newDocs.count()
          val changedCount = changedDocs.count()

          AuditLogger.audit(s"  - NEW documents: $newCount")
          AuditLogger.audit(s"  - CHANGED documents (content updated): $changedCount")


          if (newCount > 10) {
            AuditLogger.audit(s"  ... and ${newCount - 10} more new documents")
          }

          changedDocs.select("uri").limit(10).collect().foreach { row =>
            AuditLogger.audit(s"  CHANGED: ${row.getString(0)}")
          }
          if (changedCount > 10) {
            AuditLogger.audit(s"  ... and ${changedCount - 10} more changed documents")
          }
        }

        newOrChanged
      }

      val toUpsertCount = toUpsert.count()

      // Upsert into Delta Lake
      if (toUpsertCount > 0) {
        if (DeltaTable.isDeltaTable(spark, deltaPath)) {
          AuditLogger.audit(s"Upserting $toUpsertCount documents into existing Delta table")
          val deltaTable = DeltaTable.forPath(spark, deltaPath)

          val beforeCount = spark.read.format("delta").load(deltaPath).count()

          deltaTable.as("t")
            .merge(toUpsert.as("s"), "t.docId = s.docId")
            .whenMatched().updateAll()
            .whenNotMatched().insertAll()
            .execute()

          val afterCount = spark.read.format("delta").load(deltaPath).count()
          AuditLogger.audit(s"Delta table before: $beforeCount docs, after: $afterCount docs")
          AuditLogger.audit(s"Net change: +${afterCount - beforeCount} documents")
        } else {
          AuditLogger.audit(s"Creating new Delta table with $toUpsertCount documents")
          toUpsert.write.format("delta").mode("overwrite").save(deltaPath)
        }

        AuditLogger.audit(s"✅ STEP 1 COMPLETE")
      } else {
        AuditLogger.audit("✅ STEP 1 COMPLETE: No changes needed")
      }

      // Summary
      val finalCount = spark.read.format("delta").load(deltaPath).count()
      AuditLogger.audit(s"Final document count in Delta table: $finalCount")
      AuditLogger.audit("-" * 80)

      // Chain to next step if not running standalone
      if (!Config.isEMR || sys.env.get("RUN_FULL_PIPELINE").contains("true")) {
        com.rag.RAGChunks.main(Array.empty)
      }

    } catch {
      case e: Exception =>
        AuditLogger.audit(s"❌ STEP 1 FAILED: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      AuditLogger.saveLogsToS3(spark, "RAGDocNormalized")
      spark.stop()
    }
  }
}