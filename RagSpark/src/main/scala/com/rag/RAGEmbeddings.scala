package com.rag

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import io.delta.tables._
import scala.util.{Try, Success, Failure}
import scala.collection.mutable.ArrayBuffer

/**
 * RAGEmbeddings: Generates vector embeddings for document chunks
 *
 * This object handles the third step in the RAG pipeline:
 * - Loads document chunks from Delta table
 * - Identifies chunks that need embeddings (new or modified)
 * - Generates vector embeddings using Ollama service
 * - Stores embeddings in Delta table for downstream indexing
 * - Supports incremental updates by tracking chunk hashes
 * - Can skip embedding generation on EMR if embeddings are pre-computed
 * - Chains to the next pipeline step (Lucene indexing) upon completion
 */
object RAGEmbeddings {

  // Configuration values loaded from Config object
  val embedModel = Config.embedModel      // Name of the embedding model (e.g., "mxbai-embed-large")
  val embedVer = Config.embedVer          // Version of the embedding model
  val batchSize = Config.batchSize        // Number of chunks to embed in each batch
  val embPath = Config.embeddingsPath     // Path to Delta table storing embeddings
  val chunksPath = Config.chunksPath      // Path to Delta table containing document chunks

  /**
   * Main entry point for embedding generation
   *
   * Orchestrates the embedding generation pipeline:
   * 1. Loads document chunks from Delta table
   * 2. Determines which chunks need embeddings by comparing with existing embeddings
   * 3. Generates embeddings in batches using Ollama service
   * 4. Normalizes vectors using L2 normalization
   * 5. Upserts embeddings into Delta table
   * 6. Chains to next pipeline step if configured
   */
  def main(args: Array[String]): Unit = {
    // Log the start of this pipeline step
    AuditLogger.audit("STEP 3: Embedding Generation")
    AuditLogger.audit(s"Running on: ${if (Config.isEMR) "EMR" else "Local"}")

    // Initialize Spark session with Delta Lake support
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("RAGEmbeddings")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.master", if (Config.isEMR) "yarn" else "local[*]")  // Use YARN on EMR, local otherwise
      .getOrCreate()

    import spark.implicits._

    try {
      // Optional debug logging (currently commented out)
      // Can be enabled to troubleshoot path configuration issues
      //      AuditLogger.audit(s"Input path: $chunksPath")
      //      AuditLogger.audit(s"Output path: $embPath")

      // Load all document chunks from Delta table
      val chunksDF = spark.read.format("delta").load(chunksPath)
      val totalChunks = chunksDF.count()
      AuditLogger.audit(s"Loaded $totalChunks chunks from rag.chunks")
      AuditLogger.audit(s"Using embedding model: $embedModel v$embedVer")

      // Determine which chunks need embeddings
      // Only embed chunks that are new or have been modified (different hash)
      val toEmbedDF = if (DeltaTable.isDeltaTable(spark, embPath)) {
        // Embeddings table exists - perform incremental update

        // Load existing embeddings for this specific model and version
        val existing = DeltaTable.forPath(spark, embPath).toDF
          .filter($"embedder" === embedModel && $"embedder_ver" === embedVer)
          .select("chunkId", "chunkHash").distinct()

        val existingCount = existing.count()
        AuditLogger.audit(s"Found existing embeddings table with $existingCount chunks")

        // Find chunks that don't have embeddings yet or have been modified
        // Left anti join returns chunks from left that have no matching (chunkId, chunkHash) on right
        val needsEmbedding = chunksDF.join(existing, Seq("chunkId", "chunkHash"), "left_anti")
        needsEmbedding
      } else {
        // No embeddings table exists - embed all chunks
        AuditLogger.audit("No existing embeddings table - will embed all chunks")
        chunksDF
      }

      val toEmbedCount = toEmbedDF.count()

      // If no chunks need embeddings, skip to next step
      if (toEmbedCount == 0) {
        AuditLogger.audit("✅ NO CHUNKS NEED EMBEDDING - All embeddings are up to date")
        AuditLogger.audit("✅ STEP 3 COMPLETE: No embedding generation needed")
        AuditLogger.audit("-" * 80)

        // Chain to next step (Lucene indexing) if running full pipeline
        if (!Config.isEMR || sys.env.get("RUN_FULL_PIPELINE").contains("true")) {
          com.rag.RagLuceneIndex.main(Array.empty)
        }

        return
      }

      AuditLogger.audit(s"Found $toEmbedCount chunks needing embeddings (new or content changed)")

      // Check if embedding generation should be skipped
      // This is useful on EMR where Ollama may not be available
      // Embeddings can be pre-computed locally and uploaded to S3 instead
      val skipEmbedding = sys.env.get("SKIP_EMBEDDING").contains("true")

      if (skipEmbedding) {
        AuditLogger.audit("⚠️  SKIP_EMBEDDING=true - Embeddings will not be generated")
        AuditLogger.audit("   This is normal on EMR if embeddings are pre-computed locally")
        AuditLogger.audit("✅ STEP 3 COMPLETE: Skipped embedding generation")
        AuditLogger.audit("-" * 80)

        // Chain to next step even though embeddings were skipped
        // Assumes embeddings were already generated and uploaded separately
        if (!Config.isEMR || sys.env.get("RUN_FULL_PIPELINE").contains("true")) {
          com.rag.RagLuceneIndex.main(Array.empty)
        }

        return
      }

      // Warn if generating embeddings on EMR
      // EMR typically doesn't have Ollama service running
      if (Config.isEMR) {
        AuditLogger.audit("⚠️  WARNING: Running embeddings on EMR requires Ollama service")
        AuditLogger.audit("   Consider pre-computing embeddings locally and uploading to S3")
      }

      // Initialize Ollama client for embedding generation
      val ollama = new Ollama()

      // Collect chunks that need embeddings
      // Each row contains chunkId, text content, and hash for change detection
      val rows = toEmbedDF.select("chunkId", "chunk", "chunkHash").collect()

      // ArrayBuffer to accumulate successfully generated embeddings
      // Stores tuples of (chunkId, chunkHash, modelName, normalizedVector)
      val results = ArrayBuffer[(String, String, String, Array[Float])]()

      // Batch processing variables
      var i = 0  // Current position in the rows array
      val totalBatches = math.ceil(rows.length.toDouble / batchSize).toInt

      AuditLogger.audit(s"Generating embeddings in $totalBatches batches (batch size: $batchSize)")

      // Process chunks in batches to avoid memory issues and rate limiting
      while (i < rows.length) {
        // Extract current batch of chunks
        val batch = rows.slice(i, i + batchSize)

        // Extract text content for embedding
        val texts = batch.map(_.getAs[String]("chunk")).toVector

        val batchNum = i / batchSize + 1

        println(s"Processing batch $batchNum/$totalBatches (${texts.size} chunks)")

        // Attempt to generate embeddings for this batch
        // Using Try for graceful error handling - one batch failure doesn't stop the entire process
        Try(ollama.embed(texts, embedModel)) match {
          case Success(vectors) =>
            // Successfully generated embeddings for this batch

            // Process each chunk and its corresponding vector
            batch.zip(vectors).foreach { case (r, v) =>
              val cid = r.getAs[String]("chunkId")
              val hash = r.getAs[String]("chunkHash")

              // Normalize the vector using L2 normalization
              // This ensures all vectors have unit length for better similarity comparisons
              val norm = Vectors.l2(v)

              // Add to results buffer
              results += ((cid, hash, embedModel, norm))
            }
            println(s"Batch $batchNum: Successfully embedded ${vectors.length} chunks")

          case Failure(e) =>
            // Batch failed - log error but continue processing remaining batches
            println(s"Batch $batchNum failed: ${e.getMessage}")
            AuditLogger.audit(s"  WARNING: Batch $batchNum failed to embed - ${e.getMessage}")
        }

        // Move to next batch
        i += batchSize
      }

      // Calculate success and failure statistics
      val successCount = results.size
      val failCount = rows.length - successCount
      AuditLogger.audit(s"Embedding results: $successCount successful, $failCount failed")

      // Verify at least some embeddings were generated successfully
      if (results.isEmpty) {
        AuditLogger.audit("❌ ERROR: No embeddings were generated successfully")
        AuditLogger.audit("   Check Ollama service and model availability")
        throw new RuntimeException("Embedding generation failed completely")
      }

      // Convert results buffer to DataFrame
      // Add metadata columns for model version and timestamp
      val embDF = results.toSeq.toDF("chunkId", "chunkHash", "embedder", "vector")
        .withColumn("embedder_ver", lit(embedVer))                // Model version
        .withColumn("version_ts", current_timestamp())            // When embeddings were generated

      // Upsert embeddings into Delta Lake table
      if (DeltaTable.isDeltaTable(spark, embPath)) {
        // Embeddings table exists - merge new/updated embeddings
        val beforeCount = spark.read.format("delta").load(embPath).count()
        AuditLogger.audit(s"Updating existing embeddings table (before: $beforeCount embeddings)")

        val deltaTable = DeltaTable.forPath(spark, embPath)

        // Merge operation: match on chunkId, embedder, and embedder version
        deltaTable.as("t")
          .merge(
            embDF.as("s"),
            "t.chunkId = s.chunkId AND t.embedder = s.embedder AND t.embedder_ver = s.embedder_ver"
          )
          .whenMatched().updateAll()      // Update existing embeddings (e.g., if chunk content changed)
          .whenNotMatched().insertAll()   // Insert new embeddings
          .execute()

        val afterCount = spark.read.format("delta").load(embPath).count()
        AuditLogger.audit(s"Embeddings table updated (after: $afterCount, added: ${afterCount - beforeCount})")
      } else {
        // Embeddings table doesn't exist - create it
        AuditLogger.audit(s"Creating new embeddings table with ${results.size} embeddings")
        embDF.write.format("delta").mode("overwrite").save(embPath)
      }

      // Verify final state and log completion
      val finalCount = spark.read.format("delta").load(embPath).count()
      AuditLogger.audit(s"Final embedding count in Delta table: $finalCount")
      AuditLogger.audit(s"✅ STEP 3 COMPLETE: rag.embeddings updated")
      AuditLogger.audit("-" * 80)

      // Clean up Ollama client resources
      ollama.close()

      // Chain to next step (Lucene indexing) if running full pipeline
      if (!Config.isEMR || sys.env.get("RUN_FULL_PIPELINE").contains("true")) {
        com.rag.RagLuceneIndex.main(Array.empty)
      }

    } catch {
      case e: Exception =>
        // Log fatal errors that prevent pipeline completion
        AuditLogger.audit(s"❌ STEP 3 FAILED: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      // Always save logs to S3 and stop Spark session
      // This ensures logs are preserved even if there's an error
      AuditLogger.saveLogsToS3(spark, "RAGEmbeddings")
      spark.stop()
    }
  }
}