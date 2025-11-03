package com.rag

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import io.delta.tables._
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper
import java.net.URI
import java.io.InputStream

/**
 * RAGDocNormalized: Extracts and normalizes text from PDF documents
 *
 * This object handles the first step in the RAG pipeline:
 * - Reads a list of PDF file paths (local or S3)
 * - Extracts text content from each PDF using Apache PDFBox
 * - Normalizes the extracted text (removes extra whitespace)
 * - Generates content hashes to detect document changes
 * - Supports incremental updates by only processing new or modified documents
 * - Stores normalized documents in Delta Lake format
 * - Chains to the next pipeline step (document chunking)
 */
object RAGDocNormalized {

  /**
   * Extracts text content from a PDF file
   *
   * This function:
   * - Supports multiple URI schemes (file://, s3a://, s3://, http://)
   * - Uses Apache PDFBox for PDF text extraction
   * - Normalizes whitespace (collapses multiple spaces, trims)
   * - Handles errors gracefully (returns empty string on failure)
   * - Properly closes resources in finally block
   *
   * For S3 files:
   * - Uses Hadoop FileSystem API which integrates with Spark's S3 credentials
   * - Supports both s3:// and s3a:// URI schemes
   *
   * @param uri The URI of the PDF file to extract text from
   * @return Extracted and normalized text content, or empty string if extraction fails
   */
  def extractTextFromPDF(uri: String): String = {
    var doc: PDDocument = null
    var in: InputStream = null
    try {
      // Parse the URI to determine the appropriate access method
      val inputUri = new URI(uri)

      if (uri.startsWith("s3a://") || uri.startsWith("s3://")) {
        // For S3 URIs, use Hadoop FileSystem API
        // This leverages Spark's S3 configuration and credentials
        val conf = new org.apache.hadoop.conf.Configuration()
        val fs = org.apache.hadoop.fs.FileSystem.get(inputUri, conf)
        val path = new org.apache.hadoop.fs.Path(uri)
        in = fs.open(path)
      } else {
        // For file:// or http:// URIs, use standard Java URL opening
        in = new java.net.URL(uri).openStream()
      }

      // Load the PDF document from the input stream
      doc = PDDocument.load(in)

      // Extract text from all pages using PDFBox's text stripper
      val stripper = new PDFTextStripper()
      val raw = stripper.getText(doc)

      // Normalize whitespace: replace any sequence of whitespace with single space, then trim
      raw.replaceAll("\\s+", " ").trim

    } catch {
      case e: Exception =>
        // Log warning but don't fail the entire job
        // This allows the pipeline to continue even if some PDFs are corrupted or inaccessible
        println(s"[WARN] Failed to read $uri : ${e.getMessage}")
        ""  // Return empty string for failed extractions
    } finally {
      // Always close resources to prevent memory leaks
      if (doc != null) try { doc.close() } catch { case _: Throwable => }
      if (in != null) try { in.close() } catch { case _: Throwable => }
    }
  }

  /**
   * Main entry point for document normalization process
   *
   * Orchestrates the document extraction pipeline:
   * 1. Loads a list of PDF file paths from text file
   * 2. Extracts and normalizes text from each PDF
   * 3. Generates metadata (docId, contentHash, title, language)
   * 4. Identifies new or modified documents by comparing content hashes
   * 5. Upserts documents into Delta Lake table
   * 6. Chains to next pipeline step (document chunking)
   *
   * Command line arguments:
   * args(0) - Path to text file containing list of PDF URIs (one per line)
   */
  def main(args: Array[String]): Unit = {
    val runTimestamp = java.time.LocalDateTime.now()

    // Add visual separation and log pipeline start
    println("\n\n")
    AuditLogger.audit("=" * 80)
    AuditLogger.audit(s"🚀 NEW PIPELINE RUN STARTING AT: $runTimestamp")
    AuditLogger.audit("=" * 80)
    AuditLogger.audit("STEP 1: Document Normalization")
    AuditLogger.audit("-" * 80)

    // Initialize Spark session with Delta Lake extensions
    val spark = SparkSession.builder()
      .appName("RAGDocNormalized")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      // Use YARN on EMR cluster, local mode otherwise
      .config("spark.master", if (Config.isEMR) "yarn" else "local[*]")
      .getOrCreate()

    import spark.implicits._

    try {
      // Validate command line arguments
      if (args.length < 1) {
        System.err.println("Usage: RAGDocNormalized <pdfList.txt>")
        System.exit(1)
      }

      val listPath = args(0)                          // Path to file containing PDF URIs
      val deltaPath = Config.docNormalizedPath        // Output path for normalized documents

      // Optional debug logging (currently commented out)
      // Can be enabled to troubleshoot path configuration issues
      //      AuditLogger.audit(s"Input PDF list: $listPath")
      //      AuditLogger.audit(s"Output Delta path: $deltaPath")

      // Read the list of PDF file paths
      // Repartition to 10 partitions for parallel processing
      val pdfPathsDF = spark.read.text(listPath).repartition(10).toDF("uri")
      val totalPdfs = pdfPathsDF.count()
      AuditLogger.audit(s"Total PDFs in input list: $totalPdfs")

      // Register the PDF extraction function as a Spark UDF
      // This allows it to be called on distributed DataFrame operations
      val extractTextUDF = udf(extractTextFromPDF _)

      // Load existing documents from Delta table if it exists
      // This enables incremental processing - only update changed documents
      val existingDocs = if (DeltaTable.isDeltaTable(spark, deltaPath)) {
        // Delta table exists - load existing document metadata
        val existing = spark.read.format("delta").load(deltaPath)
          .select("docId", "contentHash", "uri").distinct()
        val count = existing.count()
        AuditLogger.audit(s"Found existing Delta table with $count documents")
        existing
      } else {
        // No Delta table exists - this is the first run
        AuditLogger.audit("No existing Delta table found - this is the first run")
        spark.emptyDataFrame
      }

      // Extract and normalize text from all PDFs
      val normalizedDocs = pdfPathsDF
        .withColumn("text", extractTextUDF(col("uri")))           // Extract text using UDF
        .filter(length(col("text")) > 0)                          // Filter out failed extractions
        .withColumn("language", lit("en"))                        // Set language (currently hardcoded to English)
        .withColumn("title", regexp_extract(col("text"), "^(.{1,80})", 1))  // Use first 80 chars as title
        .withColumn("docId", sha2(col("uri"), 256))               // Generate unique ID from URI
        .withColumn("contentHash", sha2(col("text"), 256))        // Hash content for change detection
        .withColumn("version_ts", current_timestamp())            // Record processing timestamp

      // Count successful and failed extractions
      val successfulExtractions = normalizedDocs.count()
      val failedExtractions = totalPdfs - successfulExtractions
      AuditLogger.audit(s"PDF extraction: $successfulExtractions successful, $failedExtractions failed")

      // Determine which documents need to be upserted
      // Only process documents that are new or have changed content
      val toUpsert = if (existingDocs.isEmpty) {
        // First run - all documents are new
        AuditLogger.audit("All documents are NEW (first run)")
        normalizedDocs
      } else {
        // Find documents where (docId, contentHash) pair doesn't exist in Delta table
        // This identifies both new documents and documents with modified content
        val newOrChanged = normalizedDocs.join(
          existingDocs,
          Seq("docId", "contentHash"),
          "left_anti"  // Return docs from left that have no matching (docId, contentHash) on right
        )
        val count = newOrChanged.count()

        if (count == 0) {
          // No changes detected - all documents are up to date
          AuditLogger.audit("✅ NO NEW OR CHANGED DOCUMENTS - All documents are up to date")
        } else {
          // Changes detected - log detailed statistics
          AuditLogger.audit(s"Found $count NEW or CHANGED documents (out of $successfulExtractions total)")

          // Separate new documents from changed documents for detailed logging
          val existingDocIds = existingDocs.select("docId").distinct()

          // New documents: docId doesn't exist in existing table
          val newDocs = newOrChanged.join(existingDocIds, Seq("docId"), "left_anti")

          // Changed documents: docId exists but contentHash is different
          val changedDocs = newOrChanged.join(existingDocIds, Seq("docId"), "left_semi")

          val newCount = newDocs.count()
          val changedCount = changedDocs.count()

          // Log counts of new vs changed documents
          AuditLogger.audit(s"  - NEW documents: $newCount")
          AuditLogger.audit(s"  - CHANGED documents (content updated): $changedCount")

          // Log sample of changed documents (limit to 10 for readability)
          // Note: The code mentions logging new docs but only shows changed docs
          // This appears to be a minor inconsistency in the original code
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

      // Upsert documents into Delta Lake table
      if (toUpsertCount > 0) {
        if (DeltaTable.isDeltaTable(spark, deltaPath)) {
          // Delta table exists - perform merge operation
          AuditLogger.audit(s"Upserting $toUpsertCount documents into existing Delta table")
          val deltaTable = DeltaTable.forPath(spark, deltaPath)

          val beforeCount = spark.read.format("delta").load(deltaPath).count()

          // Merge operation: update existing documents, insert new ones
          deltaTable.as("t")
            .merge(toUpsert.as("s"), "t.docId = s.docId")  // Match on document ID
            .whenMatched().updateAll()     // Update existing documents (content changed)
            .whenNotMatched().insertAll()  // Insert new documents
            .execute()

          val afterCount = spark.read.format("delta").load(deltaPath).count()
          AuditLogger.audit(s"Delta table before: $beforeCount docs, after: $afterCount docs")
          AuditLogger.audit(s"Net change: +${afterCount - beforeCount} documents")
        } else {
          // Delta table doesn't exist - create it
          AuditLogger.audit(s"Creating new Delta table with $toUpsertCount documents")
          toUpsert.write.format("delta").mode("overwrite").save(deltaPath)
        }

        AuditLogger.audit(s"✅ STEP 1 COMPLETE")
      } else {
        // No changes detected - skip upsert
        AuditLogger.audit("✅ STEP 1 COMPLETE: No changes needed")
      }

      // Log final state summary
      val finalCount = spark.read.format("delta").load(deltaPath).count()
      AuditLogger.audit(s"Final document count in Delta table: $finalCount")
      AuditLogger.audit("-" * 80)

      // Chain to next step (document chunking) if running full pipeline
      // On EMR, only chain if RUN_FULL_PIPELINE environment variable is set
      // This allows running steps independently or as a complete pipeline
      if (!Config.isEMR || sys.env.get("RUN_FULL_PIPELINE").contains("true")) {
        com.rag.RAGChunks.main(Array.empty)
      }

    } catch {
      case e: Exception =>
        // Log fatal errors that prevent pipeline completion
        AuditLogger.audit(s"❌ STEP 1 FAILED: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      // Always save logs to S3 and stop Spark session
      // This ensures logs are preserved even if there's an error
      AuditLogger.saveLogsToS3(spark, "RAGDocNormalized")
      spark.stop()
    }
  }
}