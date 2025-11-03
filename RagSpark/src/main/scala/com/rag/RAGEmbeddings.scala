package com.rag

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import io.delta.tables._
import scala.util.{Try, Success, Failure}
import scala.collection.mutable.ArrayBuffer

object RAGEmbeddings {

  val embedModel = Config.embedModel      // Use Config
  val embedVer = Config.embedVer          // Use Config
  val batchSize = Config.batchSize        // Use Config
  val embPath = Config.embeddingsPath     // Use Config
  val chunksPath = Config.chunksPath      // Use Config

  def main(args: Array[String]): Unit = {
    AuditLogger.audit("STEP 3: Embedding Generation")
    AuditLogger.audit(s"Running on: ${if (Config.isEMR) "EMR" else "Local"}")

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("RAGEmbeddings")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.master", if (Config.isEMR) "yarn" else "local[*]")
      .getOrCreate()

    import spark.implicits._

    try {
//      AuditLogger.audit(s"Input path: $chunksPath")
//      AuditLogger.audit(s"Output path: $embPath")

      // Load chunks
      val chunksDF = spark.read.format("delta").load(chunksPath)
      val totalChunks = chunksDF.count()
      AuditLogger.audit(s"Loaded $totalChunks chunks from rag.chunks")
      AuditLogger.audit(s"Using embedding model: $embedModel v$embedVer")

      // Determine chunks needing embeddings
      val toEmbedDF = if (DeltaTable.isDeltaTable(spark, embPath)) {
        val existing = DeltaTable.forPath(spark, embPath).toDF
          .filter($"embedder" === embedModel && $"embedder_ver" === embedVer)
          .select("chunkId", "chunkHash").distinct()

        val existingCount = existing.count()
        AuditLogger.audit(s"Found existing embeddings table with $existingCount chunks")

        val needsEmbedding = chunksDF.join(existing, Seq("chunkId", "chunkHash"), "left_anti")
        needsEmbedding
      } else {
        AuditLogger.audit("No existing embeddings table - will embed all chunks")
        chunksDF
      }

      val toEmbedCount = toEmbedDF.count()

      if (toEmbedCount == 0) {
        AuditLogger.audit("✅ NO CHUNKS NEED EMBEDDING - All embeddings are up to date")
        AuditLogger.audit("✅ STEP 3 COMPLETE: No embedding generation needed")
        AuditLogger.audit("-" * 80)

        // Chain to next step
        if (!Config.isEMR || sys.env.get("RUN_FULL_PIPELINE").contains("true")) {
          com.rag.RagLuceneIndex.main(Array.empty)
        }

        return
      }

      AuditLogger.audit(s"Found $toEmbedCount chunks needing embeddings (new or content changed)")

      // Check if we should skip embedding generation (for EMR without Ollama)
      val skipEmbedding = sys.env.get("SKIP_EMBEDDING").contains("true")

      if (skipEmbedding) {
        AuditLogger.audit("⚠️  SKIP_EMBEDDING=true - Embeddings will not be generated")
        AuditLogger.audit("   This is normal on EMR if embeddings are pre-computed locally")
        AuditLogger.audit("✅ STEP 3 COMPLETE: Skipped embedding generation")
        AuditLogger.audit("-" * 80)

        // Chain to next step
        if (!Config.isEMR || sys.env.get("RUN_FULL_PIPELINE").contains("true")) {
          com.rag.RagLuceneIndex.main(Array.empty)
        }

        return
      }

      // Generate embeddings (only if Ollama is available - typically local mode)
      if (Config.isEMR) {
        AuditLogger.audit("⚠️  WARNING: Running embeddings on EMR requires Ollama service")
        AuditLogger.audit("   Consider pre-computing embeddings locally and uploading to S3")
      }

      val ollama = new Ollama()
      val rows = toEmbedDF.select("chunkId", "chunk", "chunkHash").collect()

      val results = ArrayBuffer[(String, String, String, Array[Float])]()
      var i = 0
      val totalBatches = math.ceil(rows.length.toDouble / batchSize).toInt

      AuditLogger.audit(s"Generating embeddings in $totalBatches batches (batch size: $batchSize)")

      while (i < rows.length) {
        val batch = rows.slice(i, i + batchSize)
        val texts = batch.map(_.getAs[String]("chunk")).toVector
        val batchNum = i / batchSize + 1

        println(s"Processing batch $batchNum/$totalBatches (${texts.size} chunks)")

        Try(ollama.embed(texts, embedModel)) match {
          case Success(vectors) =>
            batch.zip(vectors).foreach { case (r, v) =>
              val cid = r.getAs[String]("chunkId")
              val hash = r.getAs[String]("chunkHash")
              val norm = Vectors.l2(v)
              results += ((cid, hash, embedModel, norm))
            }
            println(s"Batch $batchNum: Successfully embedded ${vectors.length} chunks")
          case Failure(e) =>
            println(s"Batch $batchNum failed: ${e.getMessage}")
            AuditLogger.audit(s"  WARNING: Batch $batchNum failed to embed - ${e.getMessage}")
        }
        i += batchSize
      }

      val successCount = results.size
      val failCount = rows.length - successCount
      AuditLogger.audit(s"Embedding results: $successCount successful, $failCount failed")

      if (results.isEmpty) {
        AuditLogger.audit("❌ ERROR: No embeddings were generated successfully")
        AuditLogger.audit("   Check Ollama service and model availability")
        throw new RuntimeException("Embedding generation failed completely")
      }

      // Create DataFrame from results
      val embDF = results.toSeq.toDF("chunkId", "chunkHash", "embedder", "vector")
        .withColumn("embedder_ver", lit(embedVer))
        .withColumn("version_ts", current_timestamp())

      // Upsert into Delta Lake
      if (DeltaTable.isDeltaTable(spark, embPath)) {
        val beforeCount = spark.read.format("delta").load(embPath).count()
        AuditLogger.audit(s"Updating existing embeddings table (before: $beforeCount embeddings)")

        val deltaTable = DeltaTable.forPath(spark, embPath)
        deltaTable.as("t")
          .merge(
            embDF.as("s"),
            "t.chunkId = s.chunkId AND t.embedder = s.embedder AND t.embedder_ver = s.embedder_ver"
          )
          .whenMatched().updateAll()
          .whenNotMatched().insertAll()
          .execute()

        val afterCount = spark.read.format("delta").load(embPath).count()
        AuditLogger.audit(s"Embeddings table updated (after: $afterCount, added: ${afterCount - beforeCount})")
      } else {
        AuditLogger.audit(s"Creating new embeddings table with ${results.size} embeddings")
        embDF.write.format("delta").mode("overwrite").save(embPath)
      }

      val finalCount = spark.read.format("delta").load(embPath).count()
      AuditLogger.audit(s"Final embedding count in Delta table: $finalCount")
      AuditLogger.audit(s"✅ STEP 3 COMPLETE: rag.embeddings updated")
      AuditLogger.audit("-" * 80)

      ollama.close()

      // Chain to next step
      if (!Config.isEMR || sys.env.get("RUN_FULL_PIPELINE").contains("true")) {
        com.rag.RagLuceneIndex.main(Array.empty)
      }

    } catch {
      case e: Exception =>
        AuditLogger.audit(s"❌ STEP 3 FAILED: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      AuditLogger.saveLogsToS3(spark, "RAGEmbeddings")
      spark.stop()
    }
  }
}