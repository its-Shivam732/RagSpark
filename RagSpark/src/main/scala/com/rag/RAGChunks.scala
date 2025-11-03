package com.rag

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import io.delta.tables._
import java.security.MessageDigest

object RAGChunks {

  def sha256(s: String): String = {
    MessageDigest.getInstance("SHA-256")
      .digest(s.getBytes("UTF-8"))
      .map("%02x".format(_)).mkString
  }

  def chunkDocs(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val splitUdf = udf { text: String =>
      if (text == null || text.isEmpty) Seq.empty[String]
      else Chunker.split(text, maxChars = 1200, overlap = 200)
    }

    val exploded = df
      .withColumn("chunks", splitUdf(col("text")))
      .select(
        col("docId"),
        col("contentHash").as("docHash"),
        col("language"),
        col("title"),
        posexplode(col("chunks")).as(Seq("offset", "chunk"))
      )

    exploded
      .withColumn(
        "chunkId",
        sha2(concat_ws(":", col("docId"), col("offset").cast("string")), 256)
      )
      .withColumn("chunkHash", sha2(col("chunk"), 256))
      .withColumn("version_ts", current_timestamp())
      .select(
        "chunkId", "docId", "docHash", "offset", "chunk",
        "chunkHash", "language", "title", "version_ts"
      )
  }

  def main(args: Array[String]): Unit = {
    AuditLogger.audit("STEP 2: Document Chunking")

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("RAGChunks")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.master", if (Config.isEMR) "yarn" else "local[*]")
      .getOrCreate()

    import spark.implicits._

    try {
      val docPath = Config.docNormalizedPath    // Use Config
      val chunksPath = Config.chunksPath        // Use Config

//      AuditLogger.audit(s"Input path: $docPath")
//      AuditLogger.audit(s"Output path: $chunksPath")

      // Load documents
      val docDF = spark.read.format("delta").load(docPath)
      val totalDocs = docDF.count()
      AuditLogger.audit(s"Loaded $totalDocs documents from rag.doc_normalized")

      // Check existing chunks
      val existingDocHashes = if (DeltaTable.isDeltaTable(spark, chunksPath)) {
        val existing = spark.read.format("delta").load(chunksPath)
          .select("docId", "docHash").distinct()
        val count = existing.count()
        AuditLogger.audit(s"Found existing chunks table with $count unique documents")
        existing
      } else {
        AuditLogger.audit("No existing chunks table - this is the first run")
        spark.emptyDataFrame
      }

      // Identify changed documents
      val changedDocs = if (existingDocHashes.isEmpty) {
        AuditLogger.audit("All documents need chunking (first run)")
        docDF
      } else {
        val changed = docDF.join(
          existingDocHashes,
          docDF("docId") === existingDocHashes("docId") &&
            docDF("contentHash") === existingDocHashes("docHash"),
          "left_anti"
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

      if (changedDocCount == 0) {
        AuditLogger.audit("✅ STEP 2 COMPLETE: No chunking needed")
        AuditLogger.audit("-" * 80)

        // Chain to next step
        if (!Config.isEMR || sys.env.get("RUN_FULL_PIPELINE").contains("true")) {
          com.rag.RAGEmbeddings.main(Array.empty)
        }

        return
      }

      // Chunk the changed documents
      val allNewChunks = chunkDocs(changedDocs)
      val newChunkCount = allNewChunks.count()
      AuditLogger.audit(s"Generated $newChunkCount chunks from $changedDocCount documents")

      // Skip unchanged chunks
      val existingChunks = if (DeltaTable.isDeltaTable(spark, chunksPath)) {
        spark.read.format("delta").load(chunksPath)
          .select("chunkId", "chunkHash").distinct()
      } else {
        spark.emptyDataFrame
      }

      val toUpsert = if (existingChunks.isEmpty) {
        allNewChunks
      } else {
        allNewChunks.join(existingChunks, Seq("chunkId", "chunkHash"), "left_anti")
      }

      val toUpsertCount = toUpsert.count()
      AuditLogger.audit(s"Chunks to upsert: $toUpsertCount (new or content changed)")

      // Upsert into Delta Lake
      if (toUpsertCount > 0) {
        if (DeltaTable.isDeltaTable(spark, chunksPath)) {
          val beforeCount = spark.read.format("delta").load(chunksPath).count()
          AuditLogger.audit(s"Updating existing chunks table (before: $beforeCount chunks)")

          val deltaTable = DeltaTable.forPath(spark, chunksPath)
          deltaTable.as("t")
            .merge(toUpsert.as("s"), "t.chunkId = s.chunkId")
            .whenMatched().updateAll()
            .whenNotMatched().insertAll()
            .execute()

          val afterCount = spark.read.format("delta").load(chunksPath).count()
          AuditLogger.audit(s"Chunks table updated (after: $afterCount chunks, added: ${afterCount - beforeCount})")
        } else {
          AuditLogger.audit(s"Creating new chunks table with $toUpsertCount chunks")
          toUpsert.write.format("delta").mode("overwrite").save(chunksPath)
        }

        AuditLogger.audit(s"✅ STEP 2 COMPLETE: rag.chunks updated")
      }

      val finalCount = spark.read.format("delta").load(chunksPath).count()
      AuditLogger.audit(s"Final chunk count in Delta table: $finalCount")
      AuditLogger.audit("-" * 80)

      // Chain to next step
      if (!Config.isEMR || sys.env.get("RUN_FULL_PIPELINE").contains("true")) {
        com.rag.RAGEmbeddings.main(Array.empty)
      }

    } catch {
      case e: Exception =>
        AuditLogger.audit(s"❌ STEP 2 FAILED: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      AuditLogger.saveLogsToS3(spark, "RAGChunks")
      spark.stop()
    }
  }
}