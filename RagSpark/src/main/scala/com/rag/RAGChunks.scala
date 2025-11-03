package com.rag

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import io.delta.tables._
import java.security.MessageDigest

/**
 * RAGChunks: Splits documents into manageable chunks for embedding and indexing
 *
 * This object handles the second step in the RAG pipeline:
 * - Loads normalized documents from Delta table
 * - Identifies documents that need chunking (new or modified)
 * - Splits documents into overlapping text chunks for better context preservation
 * - Generates unique IDs and content hashes for each chunk
 * - Supports incremental updates by detecting content changes
 * - Chains to the next pipeline step (embedding generation)
 */
object RAGChunks {

  /**
   * Computes SHA-256 hash of a string
   *
   * Used for generating content hashes to detect when document content has changed.
   * This enables incremental processing - only re-chunk documents when content changes.
   *
   * @param s The input string to hash
   * @return Hexadecimal string representation of the SHA-256 hash
   */
  def sha256(s: String): String = {
    MessageDigest.getInstance("SHA-256")
      .digest(s.getBytes("UTF-8"))                // Convert string to bytes and compute hash
      .map("%02x".format(_)).mkString             // Convert bytes to hex string
  }

  /**
   * Chunks documents into overlapping text segments
   *
   * This function:
   * - Splits each document's text into manageable chunks (max 1200 chars)
   * - Uses overlap (200 chars) to preserve context across chunk boundaries
   * - Assigns unique IDs to each chunk based on document ID and position
   * - Computes content hashes for change detection
   * - Preserves metadata (language, title) for each chunk
   *
   * Chunking strategy:
   * - Maximum chunk size: 1200 characters (fits within embedding model limits)
   * - Overlap: 200 characters (ensures context isn't lost at boundaries)
   * - Position-based IDs: Stable chunk IDs even when content changes
   *
   * @param df DataFrame containing documents with columns: docId, text, contentHash, language, title
   * @param spark Implicit SparkSession for DataFrame operations
   * @return DataFrame of chunks with columns: chunkId, docId, docHash, offset, chunk,
   *         chunkHash, language, title, version_ts
   */
  def chunkDocs(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    // User-defined function to split text into chunks
    // Returns empty sequence if text is null or empty
    val splitUdf = udf { text: String =>
      if (text == null || text.isEmpty) Seq.empty[String]
      else Chunker.split(text, maxChars = 1200, overlap = 200)
    }

    // Apply chunking and explode array of chunks into separate rows
    val exploded = df
      .withColumn("chunks", splitUdf(col("text")))  // Split text into array of chunks
      .select(
        col("docId"),
        col("contentHash").as("docHash"),           // Rename for consistency
        col("language"),
        col("title"),
        // posexplode creates two columns: position index (offset) and chunk text
        // This preserves the order of chunks within the document
        posexplode(col("chunks")).as(Seq("offset", "chunk"))
      )

    // Add derived columns for chunk identification and change detection
    exploded
      .withColumn(
        "chunkId",
        // Generate deterministic chunk ID from docId and position
        // This ensures same chunk always gets same ID, enabling updates
        sha2(concat_ws(":", col("docId"), col("offset").cast("string")), 256)
      )
      .withColumn("chunkHash", sha2(col("chunk"), 256))  // Hash of chunk content for change detection
      .withColumn("version_ts", current_timestamp())      // Timestamp when chunk was created
      .select(
        "chunkId", "docId", "docHash", "offset", "chunk",
        "chunkHash", "language", "title", "version_ts"
      )
  }

  /**
   * Main entry point for document chunking process
   *
   * Orchestrates the chunking pipeline:
   * 1. Loads normalized documents from Delta table
   * 2. Identifies documents that need chunking (new or content changed)
   * 3. Generates chunks with overlap for context preservation
   * 4. Filters out chunks that haven't changed (optimization)
   * 5. Upserts new/modified chunks into Delta table
   * 6. Chains to next pipeline step (embedding generation)
   */
  def main(args: Array[String]): Unit = {
    // Log the start of this pipeline step
    AuditLogger.audit("STEP 2: Document Chunking")

    // Initialize Spark session with Delta Lake support
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("RAGChunks")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.master", if (Config.isEMR) "yarn" else "local[*]")  // Use YARN on EMR, local otherwise
      .getOrCreate()

    import spark.implicits._

    try {
      // Load configuration paths
      val docPath = Config.docNormalizedPath    // Path to normalized documents Delta table
      val chunksPath = Config.chunksPath        // Path to chunks Delta table

      // Optional debug logging (currently commented out)
      // Can be enabled to troubleshoot path configuration issues
      //      AuditLogger.audit(s"Input path: $docPath")
      //      AuditLogger.audit(s"Output path: $chunksPath")

      // Load all normalized documents from Delta table
      val docDF = spark.read.format("delta").load(docPath)
      val totalDocs = docDF.count()
      AuditLogger.audit(s"Loaded $totalDocs documents from rag.doc_normalized")

      // Load existing chunk metadata to determine what needs re-chunking
      // We track (docId, docHash) pairs to detect when document content has changed
      val existingDocHashes = if (DeltaTable.isDeltaTable(spark, chunksPath)) {
        // Chunks table exists - load document hashes from existing chunks
        val existing = spark.read.format("delta").load(chunksPath)
          .select("docId", "docHash").distinct()  // One row per unique document
        val count = existing.count()
        AuditLogger.audit(s"Found existing chunks table with $count unique documents")
        existing
      } else {
        // No chunks table exists - this is the first run
        AuditLogger.audit("No existing chunks table - this is the first run")
        spark.emptyDataFrame
      }

      // Identify documents that need chunking
      // Only re-chunk documents that are new or have changed content
      val changedDocs = if (existingDocHashes.isEmpty) {
        // First run - chunk all documents
        AuditLogger.audit("All documents need chunking (first run)")
        docDF
      } else {
        // Find documents where (docId, contentHash) pair doesn't exist in chunks table
        // This includes both new documents and documents with modified content
        val changed = docDF.join(
          existingDocHashes,
          docDF("docId") === existingDocHashes("docId") &&
            docDF("contentHash") === existingDocHashes("docHash"),
          "left_anti"  // Return docs from left that have no match on right
        )
        val count = changed.count()

        if (count == 0) {
          AuditLogger.audit("✅ NO DOCUMENTS NEED RE-CHUNKING - All chunks are up to date")
        } else {
          AuditLogger.audit(s"Found $count documents needing chunking (new or content changed)")
        }

        changed
      }

      val changedDocCount = changedDocs.count()

      // If no documents need chunking, skip to next step
      if (changedDocCount == 0) {
        AuditLogger.audit("✅ STEP 2 COMPLETE: No chunking needed")
        AuditLogger.audit("-" * 80)

        // Chain to next step (embedding generation) if running full pipeline
        if (!Config.isEMR || sys.env.get("RUN_FULL_PIPELINE").contains("true")) {
          com.rag.RAGEmbeddings.main(Array.empty)
        }

        return
      }

      // Chunk all documents that need processing
      val allNewChunks = chunkDocs(changedDocs)
      val newChunkCount = allNewChunks.count()
      AuditLogger.audit(s"Generated $newChunkCount chunks from $changedDocCount documents")

      // Optimization: Skip chunks that haven't actually changed
      // Even if a document changed, some chunks might be identical to previous version
      // Load existing chunk hashes to filter out unchanged chunks
      val existingChunks = if (DeltaTable.isDeltaTable(spark, chunksPath)) {
        spark.read.format("delta").load(chunksPath)
          .select("chunkId", "chunkHash").distinct()
      } else {
        spark.emptyDataFrame
      }

      // Filter to only chunks that are new or have changed content
      val toUpsert = if (existingChunks.isEmpty) {
        // No existing chunks - upsert all
        allNewChunks
      } else {
        // Find chunks where (chunkId, chunkHash) pair doesn't exist in chunks table
        // This filters out chunks with unchanged content even if parent document changed
        allNewChunks.join(existingChunks, Seq("chunkId", "chunkHash"), "left_anti")
      }

      val toUpsertCount = toUpsert.count()
      AuditLogger.audit(s"Chunks to upsert: $toUpsertCount (new or content changed)")

      // Upsert chunks into Delta Lake table
      if (toUpsertCount > 0) {
        if (DeltaTable.isDeltaTable(spark, chunksPath)) {
          // Chunks table exists - perform merge operation
          val beforeCount = spark.read.format("delta").load(chunksPath).count()
          AuditLogger.audit(s"Updating existing chunks table (before: $beforeCount chunks)")

          val deltaTable = DeltaTable.forPath(spark, chunksPath)

          // Merge operation: update existing chunks, insert new ones
          deltaTable.as("t")
            .merge(toUpsert.as("s"), "t.chunkId = s.chunkId")  // Match on chunk ID
            .whenMatched().updateAll()     // Update existing chunks (e.g., content changed)
            .whenNotMatched().insertAll()  // Insert new chunks
            .execute()

          val afterCount = spark.read.format("delta").load(chunksPath).count()
          AuditLogger.audit(s"Chunks table updated (after: $afterCount chunks, added: ${afterCount - beforeCount})")
        } else {
          // Chunks table doesn't exist - create it
          AuditLogger.audit(s"Creating new chunks table with $toUpsertCount chunks")
          toUpsert.write.format("delta").mode("overwrite").save(chunksPath)
        }

        AuditLogger.audit(s"✅ STEP 2 COMPLETE: rag.chunks updated")
      }

      // Verify final state and log completion
      val finalCount = spark.read.format("delta").load(chunksPath).count()
      AuditLogger.audit(s"Final chunk count in Delta table: $finalCount")
      AuditLogger.audit("-" * 80)

      // Chain to next step (embedding generation) if running full pipeline
      if (!Config.isEMR || sys.env.get("RUN_FULL_PIPELINE").contains("true")) {
        com.rag.RAGEmbeddings.main(Array.empty)
      }

    } catch {
      case e: Exception =>
        // Log fatal errors that prevent pipeline completion
        AuditLogger.audit(s"❌ STEP 2 FAILED: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      // Always save logs to S3 and stop Spark session
      // This ensures logs are preserved even if there's an error
      AuditLogger.saveLogsToS3(spark, "RAGChunks")
      spark.stop()
    }
  }
}