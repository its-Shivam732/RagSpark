package com.rag

object Config {
  // Determine if running on EMR or local
  val isEMR: Boolean = sys.env.get("EMR_CLUSTER_ID").isDefined ||
    sys.props.get("spark.master").exists(_.startsWith("yarn"))

  // Base paths
  val basePath: String = if (isEMR) {
    sys.env.getOrElse("S3_BUCKET", "s3a://sparkrmr/rag-data")  // ← Changed to s3a://
  } else {
    "file:///Users/moudgil/output"
  }

  // Delta table paths
  val docNormalizedPath = s"$basePath/rag.doc_normalized"
  val chunksPath = s"$basePath/rag.chunks"
  val embeddingsPath = s"$basePath/rag.embeddings"
  val indexPath = s"$basePath/lucene_index"
  val indexedMetadataPath = s"$basePath/rag.indexed_chunks"
  val logsPath = s"$basePath/logs"

  // Spark settings
  val numShards: Int = sys.env.getOrElse("NUM_SHARDS", if (isEMR) "16" else "4").toInt
  val embedModel: String = sys.env.getOrElse("EMBED_MODEL", "mxbai-embed-large")
  val embedVer: String = sys.env.getOrElse("EMBED_VER", "1.3.0")
  val batchSize: Int = sys.env.getOrElse("BATCH_SIZE", "32").toInt
}