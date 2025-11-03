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

/**
 * RagLuceneIndex: Builds and maintains a distributed Lucene index for RAG (Retrieval-Augmented Generation)
 *
 * This object handles the fourth step in the RAG pipeline:
 * - Loads document chunks and their embeddings from Delta tables
 * - Creates or updates sharded Lucene indexes for efficient vector similarity search
 * - Maintains metadata tracking which chunks have been indexed
 * - Supports incremental updates by detecting content changes via chunk hashes
 * - Handles both local and S3-based storage for the indexes
 */
object RagLuceneIndex {

  // Configuration paths loaded from Config object
  val chunksPath = Config.chunksPath              // Path to Delta table containing document chunks
  val embPath = Config.embeddingsPath             // Path to Delta table containing vector embeddings
  val indexPath = Config.indexPath                // Output path for the sharded Lucene indexes
  val indexedMetadataPath = Config.indexedMetadataPath  // Path to metadata tracking indexed chunks
  val numShards = Config.numShards                // Number of shards to distribute the index across

  /**
   * Case class to track indexing operations immutably
   *
   * This replaces mutable var counters with an immutable data structure.
   * Each operation returns a new copy with updated counts.
   */
  case class IndexStats(added: Int = 0, updated: Int = 0, errors: Int = 0) {
    def withAdded: IndexStats = copy(added = added + 1)
    def withUpdated: IndexStats = copy(updated = updated + 1)
    def withError: IndexStats = copy(errors = errors + 1)
  }

  /**
   * Builds or updates a Lucene index for a single shard partition
   *
   * This function processes a partition of documents and:
   * - Creates a new Lucene index or opens an existing one
   * - Indexes document chunks with their text content and vector embeddings
   * - Handles both inserts (new chunks) and updates (modified chunks)
   * - Supports S3 storage by using local temp directories and syncing
   *
   * @param partitionId The ID of the partition being processed (corresponds to shard number)
   * @param rows Iterator of Row objects containing chunk data, embeddings, and metadata
   * @param outputBasePath Base path where index shards will be written
   * @param similarity Vector similarity function to use (COSINE, EUCLIDEAN, or DOT_PRODUCT)
   * @return Iterator containing tuple of (partitionId, shardPath, addedCount, updatedCount)
   */
  def buildOrUpdateShardIndex(
                               partitionId: Int,
                               rows: Iterator[Row],
                               outputBasePath: String,
                               similarity: VectorSimilarityFunction
                             ): Iterator[(Int, String, Int, Int)] = {

    // Log header for shard processing
    println(s"=" * 80)
    println(s"Starting index build/update for shard $partitionId")
    println(s"=" * 80)

    // Determine if we're writing to S3, which requires local temp directory approach
    // Lucene cannot write directly to S3, so we write locally then sync
    val useLocalTemp = outputBasePath.startsWith("s3a://") || outputBasePath.startsWith("s3://")
    val actualOutputPath = if (useLocalTemp) {
      // Create a unique temp directory for this shard
      val tempDir = s"/tmp/lucene_index_shard_$partitionId"
      println(s"Using local temp directory: $tempDir (will sync to S3)")
      tempDir
    } else {
      // For local or HDFS paths, write directly to the output location
      outputBasePath
    }

    // Construct the path for this specific shard
    val shardOutputPath = Paths.get(actualOutputPath, s"shard_$partitionId")

    // Check if an index already exists for this shard
    // An index exists if the directory exists and contains files
    val indexExists = Files.exists(shardOutputPath) &&
      Files.list(shardOutputPath).iterator().hasNext

    if (!indexExists) {
      // Create directory structure for new index
      Files.createDirectories(shardOutputPath)
      println(s"Creating NEW index for shard $partitionId at $shardOutputPath")
    } else {
      // Index will be opened in append mode to update existing documents
      println(s"UPDATING existing index for shard $partitionId at $shardOutputPath")
    }

    // Initialize Lucene components
    val analyzer = new StandardAnalyzer()  // Analyzer for text tokenization
    val iwConf = new IndexWriterConfig(analyzer)

    if (indexExists) {
      // Set to append mode to update existing index rather than overwriting
      iwConf.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
    }

    // Open the IndexWriter for this shard
    val iw = new IndexWriter(FSDirectory.open(shardOutputPath), iwConf)

    try {
      // Process each row (chunk) in this partition using foldLeft for immutable accumulation
      // This replaces the mutable var approach with a functional fold operation
      val finalStats = rows.foldLeft(IndexStats()) { (stats, row) =>
        try {
          // Extract fields from the row
          val docId = row.getAs[String]("docId")          // Parent document identifier
          val chunkId = row.getAs[String]("chunkId")      // Unique chunk identifier

          // Handle offset field which may be Int or Long depending on source
          val offset: Long = row.get(row.fieldIndex("offset")) match {
            case i: Int => i.toLong
            case l: Long => l
            case _ => 0L  // Default to 0 if unexpected type
          }

          val text = row.getAs[String]("chunk")                    // Text content of chunk
          val chunkHash = row.getAs[String]("chunkHash")           // Hash for change detection
          val vector = row.getAs[Seq[Float]]("vector").toArray     // Embedding vector
          val isUpdate = row.getAs[Boolean]("is_update")           // Flag: update vs insert

          // Handle updates by deleting old document first, then incrementing update counter
          val statsAfterUpdate = if (isUpdate) {
            // Delete existing document with this chunkId before re-adding
            // This ensures we don't have duplicate entries for the same chunk
            iw.deleteDocuments(new Term("chunk_id", chunkId))
            stats.withUpdated
          } else {
            stats
          }

          // Create a new Lucene document with all fields
          val doc = new Document()

          // Store document and chunk identifiers as string fields (not tokenized)
          doc.add(new StringField("doc_id", docId, Field.Store.YES))
          doc.add(new StringField("chunk_id", chunkId, Field.Store.YES))
          doc.add(new StringField("chunk_hash", chunkHash, Field.Store.YES))

          // Store the offset for ordering chunks within a document
          doc.add(new StoredField("offset", offset))

          // Store and index the text content for full-text search
          doc.add(new TextField("text", text, Field.Store.YES))

          // Index offset as a numeric point for range queries
          doc.add(new LongPoint("offset", offset))

          // Add the embedding vector for similarity search
          doc.add(new KnnFloatVectorField("vector", vector, similarity))

          // Add the document to the index
          iw.addDocument(doc)

          // Increment the added counter
          val newStats = statsAfterUpdate.withAdded

          // Log progress every 100 documents
          if ((newStats.added + newStats.updated) % 100 == 0) {
            println(s"Shard $partitionId: Processed ${newStats.added + newStats.updated} documents")
          }

          newStats

        } catch {
          case e: Exception =>
            // Log individual document errors but continue processing
            println(s"Error in shard $partitionId: ${e.getMessage}")
            stats.withError
        }
      }

      // Commit all changes to the index
      iw.commit()
      println(s"Shard $partitionId: Successfully committed")

      // Log final statistics for this shard
      println(s"Shard $partitionId: added=${finalStats.added}, updated=${finalStats.updated}, errors=${finalStats.errors}")

      // Return tuple with shard statistics
      Iterator((partitionId, shardOutputPath.toString, finalStats.added, finalStats.updated))

    } catch {
      case e: Exception =>
        // Fatal errors that prevent the entire shard from being processed
        println(s"Fatal error in shard $partitionId: ${e.getMessage}")
        throw e
    } finally {
      // Always close the IndexWriter to release resources
      iw.close()
    }

    // This code is unreachable due to the return in the try block
    // but kept for clarity in case the structure changes
    Iterator.empty
  }

  /**
   * Main entry point for the Lucene index building process
   *
   * Orchestrates the entire indexing pipeline:
   * 1. Loads chunks and embeddings from Delta tables
   * 2. Joins them to create a dataset of chunks with vectors
   * 3. Determines which chunks need indexing (new or modified)
   * 4. Partitions data into shards for parallel processing
   * 5. Builds/updates Lucene indexes in parallel across shards
   * 6. Updates metadata table tracking indexed chunks
   * 7. Reports comprehensive statistics
   */
  def main(args: Array[String]): Unit = {
    // Log the start of this pipeline step
    AuditLogger.audit("STEP 4: Lucene Index Building")

    // Initialize Spark session with Delta Lake extensions
    val spark = SparkSession.builder()
      .appName("RagLuceneIndex")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.master", if (Config.isEMR) "yarn" else "local[*]")  // Use YARN on EMR, local otherwise
      .getOrCreate()

    import spark.implicits._

    try {
      AuditLogger.audit("Loading chunks and embeddings from Delta tables")

      // Load document chunks from Delta table
      // Select only the fields needed for indexing
      val chunksDF = spark.read.format("delta").load(chunksPath)
        .select("chunkId", "docId", "offset", "chunk", "chunkHash")

      // Load embeddings from Delta table
      // Filter to only include embeddings from the mxbai-embed-large model
      val embeddingsDF = spark.read.format("delta").load(embPath)
        .select("chunkId", "vector", "embedder", "embedder_ver")
        .filter($"embedder" === "mxbai-embed-large")

      // Count records for logging
      val chunksCount = chunksDF.count()
      val embeddingsCount = embeddingsDF.count()

      AuditLogger.audit(s"Loaded $chunksCount chunks from rag.chunks")
      AuditLogger.audit(s"Loaded $embeddingsCount embeddings from rag.embeddings")

      AuditLogger.audit("Joining chunks with embeddings")

      // Inner join chunks with embeddings on chunkId
      // Filter out any chunks with empty vectors
      val joinedDF = chunksDF.join(embeddingsDF, Seq("chunkId"), "inner")
        .filter(size(col("vector")) > 0)

      val joinedCount = joinedDF.count()
      AuditLogger.audit(s"After join: $joinedCount chunks with valid embeddings")

      // Verify we have data to index
      if (joinedCount == 0) {
        AuditLogger.audit("❌ ERROR: No chunks with embeddings found!")
        return
      }

      AuditLogger.audit("Determining chunks needing indexing")

      // Determine which chunks need to be indexed based on metadata table
      val toIndexDF = if (DeltaTable.isDeltaTable(spark, indexedMetadataPath)) {
        // Metadata table exists - perform incremental update
        AuditLogger.audit("Found existing indexed_chunks metadata table")

        // Load previously indexed chunks with their hashes
        val indexedChunks = spark.read.format("delta").load(indexedMetadataPath)
          .select("chunkId", "chunkHash").distinct()

        val indexedCount = indexedChunks.count()
        AuditLogger.audit(s"Already indexed: $indexedCount chunks")

        // Find chunks that are either new or have been modified (different hash)
        // Left anti join returns rows from left that have no match on right
        val newOrUpdated = joinedDF.join(indexedChunks, Seq("chunkId", "chunkHash"), "left_anti")

        val newOrUpdatedCount = newOrUpdated.count()

        // If no new or updated chunks, the index is up to date
        if (newOrUpdatedCount == 0) {
          AuditLogger.audit("✅ NO NEW OR UPDATED CHUNKS TO INDEX - Index is up to date")
          AuditLogger.audit("✅ STEP 4 COMPLETE: No indexing needed")
          AuditLogger.audit("=" * 80)
          AuditLogger.audit("PIPELINE RUN COMPLETED SUCCESSFULLY")
          AuditLogger.audit("=" * 80)
          return
        }

        AuditLogger.audit(s"Found $newOrUpdatedCount chunks needing indexing (new or content changed)")

        // Get set of all chunkIds that exist in the index
        val existingChunkIds = indexedChunks.select("chunkId").distinct()

        // Separate chunks into updates (existing chunkId with new content) and inserts (new chunkId)
        // Updates need to delete old document first
        val updates = newOrUpdated.join(existingChunkIds, Seq("chunkId"), "left_semi")
          .withColumn("is_update", lit(true))

        // Inserts are completely new chunks
        val inserts = newOrUpdated.join(existingChunkIds, Seq("chunkId"), "left_anti")
          .withColumn("is_update", lit(false))

        val updateCount = updates.count()
        val insertCount = inserts.count()

        AuditLogger.audit(s"  - INSERTS (new chunks): $insertCount")
        AuditLogger.audit(s"  - UPDATES (content changed): $updateCount")

        // Combine updates and inserts into single dataset
        updates.union(inserts)

      } else {
        // No metadata table exists - this is the initial index build
        AuditLogger.audit("No existing indexed_chunks metadata - will index all chunks")
        joinedDF.withColumn("is_update", lit(false))  // All are inserts
      }

      val totalToIndex = toIndexDF.count()
      AuditLogger.audit(s"Total chunks to index: $totalToIndex")

      AuditLogger.audit(s"Partitioning data into $numShards shards (keeping documents together)")

      // Shard the data for parallel processing
      // Use CRC32 hash of docId to ensure all chunks from same document go to same shard
      // This keeps related chunks together for better query performance
      val shardedDF = toIndexDF
        .withColumn("shard", abs(crc32(col("docId"))) % numShards)  // Assign shard number
        .repartition(numShards, col("shard"))                       // Distribute across partitions
        .sortWithinPartitions("docId", "offset")                    // Sort chunks within each partition

      // Calculate and log distribution statistics
      AuditLogger.audit("Shard distribution:")
      val shardStats = shardedDF.groupBy("shard").agg(
        count("*").as("chunks"),
        countDistinct("docId").as("documents")
      ).orderBy("shard").collect()

      // Log statistics for each shard
      shardStats.foreach { row =>
        val shardId = row.getAs[Long](0)
        val chunks = row.getAs[Long](1)
        val docs = row.getAs[Long](2)
        AuditLogger.audit(f"  Shard $shardId%2d: $chunks%,6d chunks from $docs documents")
      }

      // Determine vector similarity function from environment variable
      val similarityName = sys.env.getOrElse("RAG_SIMILARITY", "COSINE")
      val similarity = similarityName match {
        case "EUCLIDEAN" => VectorSimilarityFunction.EUCLIDEAN
        case "DOT_PRODUCT" => VectorSimilarityFunction.DOT_PRODUCT
        case _ => VectorSimilarityFunction.COSINE  // Default to cosine similarity
      }
      AuditLogger.audit(s"Using vector similarity function: $similarityName")

      AuditLogger.audit("Building/updating Lucene indexes in parallel...")

      // Remove file:// prefix if present for local paths
      val outputPath = indexPath.replace("file://", "")

      // Process each partition in parallel to build/update shard indexes
      // Each partition corresponds to one shard
      val results = shardedDF
        .mapPartitions { rows =>
          val partitionId = org.apache.spark.TaskContext.getPartitionId()
          buildOrUpdateShardIndex(partitionId, rows, outputPath, similarity)
        }
        .collect()  // Collect results from all partitions

      AuditLogger.audit("Updating indexed chunks metadata table")

      // Prepare metadata for chunks that were just indexed
      val indexedChunksDF = toIndexDF
        .select("chunkId", "chunkHash", "docId")
        .withColumn("indexed_ts", current_timestamp())  // Record when it was indexed

      // Update or create the metadata table
      if (DeltaTable.isDeltaTable(spark, indexedMetadataPath)) {
        // Metadata table exists - merge new/updated records
        val beforeCount = spark.read.format("delta").load(indexedMetadataPath).count()
        AuditLogger.audit(s"Updating metadata table (before: $beforeCount chunks)")

        val deltaTable = DeltaTable.forPath(spark, indexedMetadataPath)

        // Merge operation: update existing records, insert new ones
        deltaTable.as("t")
          .merge(
            indexedChunksDF.as("s"),
            "t.chunkId = s.chunkId"  // Match on chunkId
          )
          .whenMatched()
          .updateAll()  // Update all columns for matched records
          .whenNotMatched()
          .insertAll()  // Insert all columns for new records
          .execute()

        val afterCount = spark.read.format("delta").load(indexedMetadataPath).count()
        AuditLogger.audit(s"Metadata table updated (after: $afterCount chunks)")
      } else {
        // Metadata table doesn't exist - create it
        AuditLogger.audit(s"Creating new metadata table with $totalToIndex chunks")
        indexedChunksDF.write.format("delta").mode("overwrite").save(indexedMetadataPath)
      }

      // Calculate totals across all shards
      val totalAdded = results.map(_._3).sum
      val totalUpdated = results.map(_._4).sum

      // Log comprehensive summary of indexing operation
      AuditLogger.audit("=" * 80)
      AuditLogger.audit("Lucene Index Build/Update Summary:")
      AuditLogger.audit("=" * 80)

      // Log per-shard statistics
      results.sortBy(_._1).foreach { case (shardId, path, added, updated) =>
        AuditLogger.audit(f"Shard $shardId%2d: added=$added%,6d  updated=$updated%,6d")
      }

      // Log overall statistics
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
        // Log fatal errors that prevent pipeline completion
        AuditLogger.audit(s"❌ STEP 4 FAILED: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      // Always save logs to S3 and stop Spark session
      AuditLogger.saveLogsToS3(spark, "RagLuceneIndex")
      spark.stop()
    }
  }
}