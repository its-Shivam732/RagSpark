package com.rag

import org.apache.spark.sql.{SparkSession, Row, DataFrame}
import org.apache.spark.sql.functions._
import io.delta.tables._
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, VectorSimilarityFunction, Term}
import org.apache.lucene.store.FSDirectory
import java.nio.file.{Files, Paths, Path => JPath}
import scala.jdk.CollectionConverters._

object RagLuceneIndex {

  val chunksPath = Config.chunksPath              // Use Config
  val embPath = Config.embeddingsPath             // Use Config
  val indexPath = Config.indexPath                // Use Config
  val indexedMetadataPath = Config.indexedMetadataPath  // Use Config
  val numShards = Config.numShards                // Use Config

  def buildOrUpdateShardIndex(
                               partitionId: Int,
                               rows: Iterator[Row],
                               outputBasePath: String,
                               similarity: VectorSimilarityFunction
                             ): Iterator[(Int, String, Int, Int)] = {

    println(s"=" * 80)
    println(s"Starting index build/update for shard $partitionId")
    println(s"=" * 80)

    // For S3, we need to use local temp directory then upload
    val useLocalTemp = outputBasePath.startsWith("s3a://") || outputBasePath.startsWith("s3://")
    val actualOutputPath = if (useLocalTemp) {
      val tempDir = s"/tmp/lucene_index_shard_$partitionId"
      println(s"Using local temp directory: $tempDir (will sync to S3)")
      tempDir
    } else {
      outputBasePath
    }

    val shardOutputPath = Paths.get(actualOutputPath, s"shard_$partitionId")

    val indexExists = Files.exists(shardOutputPath) &&
      Files.list(shardOutputPath).iterator().hasNext

    if (!indexExists) {
      Files.createDirectories(shardOutputPath)
      println(s"Creating NEW index for shard $partitionId at $shardOutputPath")
    } else {
      println(s"UPDATING existing index for shard $partitionId at $shardOutputPath")
    }

    val analyzer = new StandardAnalyzer()
    val iwConf = new IndexWriterConfig(analyzer)

    if (indexExists) {
      iwConf.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
    }

    val iw = new IndexWriter(FSDirectory.open(shardOutputPath), iwConf)

    var addedCount = 0
    var updatedCount = 0
    var errorCount = 0

    try {
      rows.foreach { row =>
        try {
          val docId = row.getAs[String]("docId")
          val chunkId = row.getAs[String]("chunkId")

          val offset: Long = row.get(row.fieldIndex("offset")) match {
            case i: Int => i.toLong
            case l: Long => l
            case _ => 0L
          }

          val text = row.getAs[String]("chunk")
          val chunkHash = row.getAs[String]("chunkHash")
          val vector = row.getAs[Seq[Float]]("vector").toArray
          val isUpdate = row.getAs[Boolean]("is_update")

          if (isUpdate) {
            iw.deleteDocuments(new Term("chunk_id", chunkId))
            updatedCount += 1
          }

          val doc = new Document()
          doc.add(new StringField("doc_id", docId, Field.Store.YES))
          doc.add(new StringField("chunk_id", chunkId, Field.Store.YES))
          doc.add(new StringField("chunk_hash", chunkHash, Field.Store.YES))
          doc.add(new StoredField("offset", offset))
          doc.add(new TextField("text", text, Field.Store.YES))
          doc.add(new LongPoint("offset", offset))
          doc.add(new KnnFloatVectorField("vector", vector, similarity))

          iw.addDocument(doc)
          addedCount += 1

          if ((addedCount + updatedCount) % 100 == 0) {
            println(s"Shard $partitionId: Processed ${addedCount + updatedCount} documents")
          }

        } catch {
          case e: Exception =>
            println(s"Error in shard $partitionId: ${e.getMessage}")
            errorCount += 1
        }
      }

      iw.commit()
      println(s"Shard $partitionId: Successfully committed")

    } catch {
      case e: Exception =>
        println(s"Fatal error in shard $partitionId: ${e.getMessage}")
        throw e
    } finally {
      iw.close()
    }

    // If using S3, sync the local temp directory to S3
    if (useLocalTemp) {
      try {
        println(s"Syncing shard $partitionId to S3: $outputBasePath")
        val s3Path = s"$outputBasePath/shard_$partitionId"

        // Use Hadoop FileSystem to copy to S3
        val conf = new org.apache.hadoop.conf.Configuration()
        val localFs = org.apache.hadoop.fs.FileSystem.getLocal(conf)
        val s3Fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(s3Path), conf)

        val localPath = new org.apache.hadoop.fs.Path(shardOutputPath.toString)
        val remotePath = new org.apache.hadoop.fs.Path(s3Path)

        org.apache.hadoop.fs.FileUtil.copy(
          localFs, localPath,
          s3Fs, remotePath,
          false, // don't delete source
          true,  // overwrite destination
          conf
        )

        println(s"Shard $partitionId synced to S3 successfully")
      } catch {
        case e: Exception =>
          println(s"Warning: Failed to sync shard $partitionId to S3: ${e.getMessage}")
      }
    }

    println(s"Shard $partitionId: added=$addedCount, updated=$updatedCount, errors=$errorCount")
    Iterator((partitionId, shardOutputPath.toString, addedCount, updatedCount))
  }

  def main(args: Array[String]): Unit = {
    AuditLogger.audit("STEP 4: Lucene Index Building")
    val spark = SparkSession.builder()
      .appName("RagLuceneIndex")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.master", if (Config.isEMR) "yarn" else "local[*]")
      .getOrCreate()

    import spark.implicits._

    try {
      AuditLogger.audit("Loading chunks and embeddings from Delta tables")

      val chunksDF = spark.read.format("delta").load(chunksPath)
        .select("chunkId", "docId", "offset", "chunk", "chunkHash")

      val embeddingsDF = spark.read.format("delta").load(embPath)
        .select("chunkId", "vector", "embedder", "embedder_ver")
        .filter($"embedder" === "mxbai-embed-large")

      val chunksCount = chunksDF.count()
      val embeddingsCount = embeddingsDF.count()

      AuditLogger.audit(s"Loaded $chunksCount chunks from rag.chunks")
      AuditLogger.audit(s"Loaded $embeddingsCount embeddings from rag.embeddings")

      AuditLogger.audit("Joining chunks with embeddings")

      val joinedDF = chunksDF.join(embeddingsDF, Seq("chunkId"), "inner")
        .filter(size(col("vector")) > 0)

      val joinedCount = joinedDF.count()
      AuditLogger.audit(s"After join: $joinedCount chunks with valid embeddings")

      if (joinedCount == 0) {
        AuditLogger.audit("❌ ERROR: No chunks with embeddings found!")
        return
      }

      AuditLogger.audit("Determining chunks needing indexing")

      val toIndexDF = if (DeltaTable.isDeltaTable(spark, indexedMetadataPath)) {
        AuditLogger.audit("Found existing indexed_chunks metadata table")

        val indexedChunks = spark.read.format("delta").load(indexedMetadataPath)
          .select("chunkId", "chunkHash").distinct()

        val indexedCount = indexedChunks.count()
        AuditLogger.audit(s"Already indexed: $indexedCount chunks")

        val newOrUpdated = joinedDF.join(indexedChunks, Seq("chunkId", "chunkHash"), "left_anti")

        val newOrUpdatedCount = newOrUpdated.count()

        if (newOrUpdatedCount == 0) {
          AuditLogger.audit("✅ NO NEW OR UPDATED CHUNKS TO INDEX - Index is up to date")
          AuditLogger.audit("✅ STEP 4 COMPLETE: No indexing needed")
          AuditLogger.audit("=" * 80)
          AuditLogger.audit("PIPELINE RUN COMPLETED SUCCESSFULLY")
          AuditLogger.audit("=" * 80)
          return
        }

        AuditLogger.audit(s"Found $newOrUpdatedCount chunks needing indexing (new or content changed)")

        val existingChunkIds = indexedChunks.select("chunkId").distinct()

        val updates = newOrUpdated.join(existingChunkIds, Seq("chunkId"), "left_semi")
          .withColumn("is_update", lit(true))

        val inserts = newOrUpdated.join(existingChunkIds, Seq("chunkId"), "left_anti")
          .withColumn("is_update", lit(false))

        val updateCount = updates.count()
        val insertCount = inserts.count()

        AuditLogger.audit(s"  - INSERTS (new chunks): $insertCount")
        AuditLogger.audit(s"  - UPDATES (content changed): $updateCount")

        updates.union(inserts)

      } else {
        AuditLogger.audit("No existing indexed_chunks metadata - will index all chunks")
        joinedDF.withColumn("is_update", lit(false))
      }

      val totalToIndex = toIndexDF.count()
      AuditLogger.audit(s"Total chunks to index: $totalToIndex")

      AuditLogger.audit(s"Partitioning data into $numShards shards (keeping documents together)")

      val shardedDF = toIndexDF
        .withColumn("shard", abs(crc32(col("docId"))) % numShards)
        .repartition(numShards, col("shard"))
        .sortWithinPartitions("docId", "offset")

      AuditLogger.audit("Shard distribution:")
      val shardStats = shardedDF.groupBy("shard").agg(
        count("*").as("chunks"),
        countDistinct("docId").as("documents")
      ).orderBy("shard").collect()

      shardStats.foreach { row =>
        val shardId = row.getAs[Long](0)
        val chunks = row.getAs[Long](1)
        val docs = row.getAs[Long](2)
        AuditLogger.audit(f"  Shard $shardId%2d: $chunks%,6d chunks from $docs documents")
      }

      val similarityName = sys.env.getOrElse("RAG_SIMILARITY", "COSINE")
      val similarity = similarityName match {
        case "EUCLIDEAN" => VectorSimilarityFunction.EUCLIDEAN
        case "DOT_PRODUCT" => VectorSimilarityFunction.DOT_PRODUCT
        case _ => VectorSimilarityFunction.COSINE
      }
      AuditLogger.audit(s"Using vector similarity function: $similarityName")

      AuditLogger.audit("Building/updating Lucene indexes in parallel...")

      val outputPath = indexPath.replace("file://", "")

      val results = shardedDF
        .mapPartitions { rows =>
          val partitionId = org.apache.spark.TaskContext.getPartitionId()
          buildOrUpdateShardIndex(partitionId, rows, outputPath, similarity)
        }
        .collect()

      AuditLogger.audit("Updating indexed chunks metadata table")

      val indexedChunksDF = toIndexDF
        .select("chunkId", "chunkHash", "docId")
        .withColumn("indexed_ts", current_timestamp())

      if (DeltaTable.isDeltaTable(spark, indexedMetadataPath)) {
        val beforeCount = spark.read.format("delta").load(indexedMetadataPath).count()
        AuditLogger.audit(s"Updating metadata table (before: $beforeCount chunks)")

        val deltaTable = DeltaTable.forPath(spark, indexedMetadataPath)
        deltaTable.as("t")
          .merge(
            indexedChunksDF.as("s"),
            "t.chunkId = s.chunkId"
          )
          .whenMatched()
          .updateAll()
          .whenNotMatched()
          .insertAll()
          .execute()

        val afterCount = spark.read.format("delta").load(indexedMetadataPath).count()
        AuditLogger.audit(s"Metadata table updated (after: $afterCount chunks)")
      } else {
        AuditLogger.audit(s"Creating new metadata table with $totalToIndex chunks")
        indexedChunksDF.write.format("delta").mode("overwrite").save(indexedMetadataPath)
      }

      val totalAdded = results.map(_._3).sum
      val totalUpdated = results.map(_._4).sum

      AuditLogger.audit("=" * 80)
      AuditLogger.audit("Lucene Index Build/Update Summary:")
      AuditLogger.audit("=" * 80)

      results.sortBy(_._1).foreach { case (shardId, path, added, updated) =>
        AuditLogger.audit(f"Shard $shardId%2d: added=$added%,6d  updated=$updated%,6d")
      }

      AuditLogger.audit("=" * 80)
      AuditLogger.audit(s"Index Statistics:")
      AuditLogger.audit(s"  Total shards: ${results.length}")
      AuditLogger.audit(s"  Total documents added: $totalAdded")
      AuditLogger.audit(s"  Total documents updated: $totalUpdated")
      AuditLogger.audit("=" * 80)
      AuditLogger.audit("✅ STEP 4 COMPLETE: Lucene index updated successfully")
      AuditLogger.audit("=" * 80)
      AuditLogger.audit("PIPELINE RUN COMPLETED SUCCESSFULLY")
      AuditLogger.audit("=" * 80)

    } catch {
      case e: Exception =>
        AuditLogger.audit(s"❌ STEP 4 FAILED: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      AuditLogger.saveLogsToS3(spark, "RagLuceneIndex")
      spark.stop()
    }
  }
}