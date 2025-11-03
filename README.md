# RAG Pipeline Documentation

## Overview

This is a complete **Retrieval-Augmented Generation (RAG)** pipeline built with Scala, Apache Spark, Delta Lake, and Apache Lucene. The pipeline processes PDF documents, generates embeddings, and builds searchable vector indexes for efficient semantic search and retrieval.

## Table of Contents

- [Architecture](#architecture)
- [Pipeline Flow](#pipeline-flow)
  - [Step 1: Document Normalization](#step-1-document-normalization-ragdocnormalized)
  - [Step 2: Document Chunking](#step-2-document-chunking-ragchunks)
  - [Step 3: Embedding Generation](#step-3-embedding-generation-ragembeddings)
  - [Step 4: Lucene Index Building](#step-4-lucene-index-building-ragluceneindex)
- [Data Flow Diagram](#data-flow-diagram)
- [Audit Logs](#audit-logs)
- [Running the Pipeline](#running-the-pipeline)
  - [Prerequisites](#prerequisites)
  - [Local Mode](#local-mode)
  - [EMR Mode](#emr-mode)
- [Configuration](#configuration)
- [Incremental Updates](#incremental-updates)
- [Project Structure](#project-structure)
- [Technology Stack](#technology-stack)

---

## Architecture

The RAG pipeline consists of four main stages that transform raw PDF documents into a searchable vector index:

```
PDFs → Normalize → Chunk → Embed → Index
  ↓        ↓         ↓       ↓       ↓
Input   Delta    Delta   Delta   Lucene
Files   Table    Table   Table   Shards
```

Each stage:
- Processes data incrementally (only new/changed content)
- Uses Delta Lake for ACID transactions and versioning
- Supports both local development and distributed EMR execution
- Maintains comprehensive audit logs

---

## Pipeline Flow

### Step 1: Document Normalization (`RAGDocNormalized`)

**Purpose**: Extract and normalize text from PDF documents.

**Input**: 
- Text file containing list of PDF URIs (one per line)
- PDFs can be from local filesystem, S3, or HTTP

**Process**:
1. Read list of PDF file paths
2. Extract text from each PDF using Apache PDFBox
3. Normalize whitespace (collapse multiple spaces, trim)
4. Generate metadata:
   - `docId`: SHA-256 hash of URI (unique document identifier)
   - `contentHash`: SHA-256 hash of text content (for change detection)
   - `title`: First 80 characters of text
   - `language`: Currently hardcoded to "en"
   - `version_ts`: Processing timestamp
5. Compare with existing documents in Delta table
6. Upsert only new or modified documents

**Output**: Delta table `rag.doc_normalized` with columns:
```
docId, uri, text, contentHash, language, title, version_ts
```

**Incremental Updates**:
- Documents are identified by (docId, contentHash)
- Only documents with new content are re-processed
- Supports both new documents and content updates

**Key Features**:
- Handles S3, local, and HTTP URIs
- Graceful error handling (corrupted PDFs don't stop pipeline)
- Parallel processing (repartitioned across 10 partitions)

---

### Step 2: Document Chunking (`RAGChunks`)

**Purpose**: Split documents into overlapping chunks for better embedding and retrieval.

**Input**: Delta table `rag.doc_normalized`

**Process**:
1. Load normalized documents
2. Identify documents needing chunking (new or content changed)
3. Split each document into chunks:
   - **Max chunk size**: 1200 characters
   - **Overlap**: 200 characters (preserves context across boundaries)
4. Generate chunk metadata:
   - `chunkId`: SHA-256(docId:offset) - deterministic, position-based ID
   - `chunkHash`: SHA-256(chunk text) - for change detection
   - `offset`: Position in document (enables proper ordering)
5. Filter out unchanged chunks (optimization)
6. Upsert chunks into Delta table

**Output**: Delta table `rag.chunks` with columns:
```
chunkId, docId, docHash, offset, chunk, chunkHash, language, title, version_ts
```

**Chunking Strategy**:
- **Why 1200 chars?** Fits within embedding model token limits while maintaining context
- **Why 200 char overlap?** Ensures sentences/concepts split across chunks are still captured
- **Position-based IDs**: Same chunk always gets same ID, enabling efficient updates

**Incremental Updates**:
- Only re-chunks documents with changed content (docHash comparison)
- Even within changed documents, filters out chunks with unchanged content
- Two-level optimization: document-level and chunk-level

---

### Step 3: Embedding Generation (`RAGEmbeddings`)

**Purpose**: Generate vector embeddings for each chunk using an embedding model.

**Input**: Delta table `rag.chunks`

**Process**:
1. Load chunks from Delta table
2. Identify chunks needing embeddings:
   - New chunks (not in embeddings table)
   - Modified chunks (different chunkHash)
3. Generate embeddings in batches:
   - Default batch size: configurable (prevents memory issues)
   - Uses Ollama service with `mxbai-embed-large` model
4. Normalize vectors (L2 normalization):
   - Ensures all vectors have unit length
   - Improves similarity comparison accuracy
5. Handle batch failures gracefully (one batch failure doesn't stop pipeline)
6. Upsert embeddings into Delta table

**Output**: Delta table `rag.embeddings` with columns:
```
chunkId, chunkHash, embedder, embedder_ver, vector, version_ts
```

**Embedding Details**:
- **Model**: mxbai-embed-large (configurable via Config)
- **Vector dimensions**: Model-dependent (typically 768 or 1024)
- **Normalization**: L2 normalization for cosine similarity
- **Batch processing**: Reduces memory usage and API rate limiting

**Skip Mode** (for EMR):
- Can set `SKIP_EMBEDDING=true` to skip generation on EMR
- Useful when embeddings are pre-computed locally and uploaded to S3
- Allows separation of compute-intensive embedding from distributed indexing

**Incremental Updates**:
- Tracks (chunkId, chunkHash, embedder, embedder_ver)
- Only generates embeddings for new/modified chunks
- Supports model upgrades (different embedder_ver triggers re-embedding)

---

### Step 4: Lucene Index Building (`RagLuceneIndex`)

**Purpose**: Build distributed Lucene indexes for efficient vector similarity search.

**Input**: 
- Delta table `rag.chunks`
- Delta table `rag.embeddings`

**Process**:
1. Join chunks with embeddings on chunkId
2. Identify chunks needing indexing:
   - New chunks (not in indexed metadata)
   - Modified chunks (different chunkHash)
3. Partition data into shards:
   - Uses CRC32(docId) % numShards
   - Keeps all chunks from same document in same shard
4. Build/update Lucene indexes in parallel:
   - Each Spark partition processes one shard
   - Creates/updates Lucene index with:
     - Full-text fields (text)
     - Vector fields (KnnFloatVectorField)
     - Metadata fields (docId, chunkId, offset)
5. For S3 storage:
   - Write to local temp directory
   - Sync to S3 using Hadoop FileSystem API
6. Update indexed metadata table

**Output**: 
- Lucene index shards in `Config.indexPath/shard_N/`
- Delta table `rag.indexed_chunks` with columns:
  ```
  chunkId, chunkHash, docId, indexed_ts
  ```

**Index Structure**:
Each Lucene document contains:
- **StringField**: doc_id, chunk_id, chunk_hash (exact match, stored)
- **TextField**: text (tokenized, searchable, stored)
- **LongPoint**: offset (range queries, not stored)
- **StoredField**: offset (stored but not indexed)
- **KnnFloatVectorField**: vector (similarity search)

**Sharding Strategy**:
- **Why shard?** Enables parallel query processing and scales horizontally
- **Why CRC32(docId)?** Deterministic - same document always goes to same shard
- **Benefit**: Related chunks stay together for better query performance

**Vector Similarity Functions**:
- **COSINE** (default): Best for normalized vectors
- **EUCLIDEAN**: Good for absolute distances
- **DOT_PRODUCT**: Fast but requires normalized vectors
- Configurable via `RAG_SIMILARITY` environment variable

**Incremental Updates**:
- Tracks (chunkId, chunkHash) in metadata table
- Distinguishes between inserts (new) and updates (modified)
- Updates delete old document and add new one (no duplicates)

**S3 Support**:
- Lucene cannot write directly to S3
- Solution: Write to local temp, then sync to S3
- Uses Hadoop FileSystem API for reliable S3 uploads

---

## Data Flow Diagram

```
┌─────────────┐
│  PDF List   │
│  (input)    │
└──────┬──────┘
       │
       ▼
┌──────────────────────────────────────────┐
│  STEP 1: Document Normalization          │
│  - Extract text from PDFs                │
│  - Normalize whitespace                  │
│  - Generate docId & contentHash          │
└──────┬───────────────────────────────────┘
       │
       ▼
┌─────────────────────┐
│  rag.doc_normalized │  ◄─── Incremental: Only new/changed docs
│  Delta Table        │
└──────┬──────────────┘
       │
       ▼
┌──────────────────────────────────────────┐
│  STEP 2: Document Chunking                │
│  - Split docs into 1200-char chunks      │
│  - 200-char overlap for context          │
│  - Generate chunkId & chunkHash          │
└──────┬───────────────────────────────────┘
       │
       ▼
┌─────────────────────┐
│    rag.chunks       │  ◄─── Incremental: Only new/changed chunks
│    Delta Table      │
└──────┬──────────────┘
       │
       ▼
┌──────────────────────────────────────────┐
│  STEP 3: Embedding Generation             │
│  - Generate vectors via Ollama           │
│  - L2 normalize vectors                  │
│  - Batch processing                      │
└──────┬───────────────────────────────────┘
       │
       ▼
┌─────────────────────┐
│  rag.embeddings     │  ◄─── Incremental: Only new/changed chunks
│  Delta Table        │
└──────┬──────────────┘
       │
       ▼
┌──────────────────────────────────────────┐
│  STEP 4: Lucene Index Building            │
│  - Join chunks + embeddings              │
│  - Shard by docId                        │
│  - Build Lucene indexes in parallel      │
│  - Sync to S3 if needed                  │
└──────┬───────────────────────────────────┘
       │
       ▼
┌─────────────────────┐
│  Lucene Index       │  ◄─── Sharded: shard_0, shard_1, ...
│  (searchable)       │       + rag.indexed_chunks metadata
└─────────────────────┘
```

---

## Audit Logs

The `AuditLogger` maintains comprehensive logs throughout the pipeline execution.

### Log Types

1. **Audit Logs** (`rag_audit.log`):
   - High-level pipeline events
   - Step start/completion
   - Document/chunk counts
   - Success/failure indicators
   - Performance metrics

2. **Pipeline Logs** (`rag_pipeline.log`):
   - Detailed processing information
   - Individual batch results
   - Error details
   - Debug information

### Log Content Examples

**Step 1 (Document Normalization)**:
```
2025-01-01 10:00:00 | 🚀 NEW PIPELINE RUN STARTING AT: 2025-01-01T10:00:00
2025-01-01 10:00:00 | STEP 1: Document Normalization
2025-01-01 10:00:05 | Total PDFs in input list: 150
2025-01-01 10:02:30 | PDF extraction: 148 successful, 2 failed
2025-01-01 10:02:35 | Found 25 NEW or CHANGED documents (out of 148 total)
2025-01-01 10:02:35 |   - NEW documents: 20
2025-01-01 10:02:35 |   - CHANGED documents (content updated): 5
2025-01-01 10:03:00 | Delta table before: 150 docs, after: 170 docs
2025-01-01 10:03:00 | ✅ STEP 1 COMPLETE
```

**Step 2 (Chunking)**:
```
2025-01-01 10:03:01 | STEP 2: Document Chunking
2025-01-01 10:03:05 | Loaded 170 documents from rag.doc_normalized
2025-01-01 10:03:06 | Found 25 documents needing chunking (new or content changed)
2025-01-01 10:03:20 | Generated 487 chunks from 25 documents
2025-01-01 10:03:21 | Chunks to upsert: 487 (new or content changed)
2025-01-01 10:03:45 | Chunks table updated (after: 3245 chunks, added: 487)
2025-01-01 10:03:45 | ✅ STEP 2 COMPLETE: rag.chunks updated
```

**Step 3 (Embeddings)**:
```
2025-01-01 10:03:46 | STEP 3: Embedding Generation
2025-01-01 10:03:50 | Loaded 3245 chunks from rag.chunks
2025-01-01 10:03:51 | Found 487 chunks needing embeddings (new or content changed)
2025-01-01 10:03:51 | Generating embeddings in 10 batches (batch size: 50)
2025-01-01 10:05:20 | Batch 1: Successfully embedded 50 chunks
...
2025-01-01 10:12:45 | Embedding results: 487 successful, 0 failed
2025-01-01 10:12:50 | Embeddings table updated (after: 3245, added: 487)
2025-01-01 10:12:50 | ✅ STEP 3 COMPLETE: rag.embeddings updated
```

**Step 4 (Indexing)**:
```
2025-01-01 10:12:51 | STEP 4: Lucene Index Building
2025-01-01 10:12:55 | Loaded 3245 chunks from rag.chunks
2025-01-01 10:12:56 | Loaded 3245 embeddings from rag.embeddings
2025-01-01 10:12:57 | After join: 3245 chunks with valid embeddings
2025-01-01 10:12:58 | Found 487 chunks needing indexing (new or content changed)
2025-01-01 10:12:58 |   - INSERTS (new chunks): 487
2025-01-01 10:12:58 |   - UPDATES (content changed): 0
2025-01-01 10:12:59 | Partitioning data into 4 shards (keeping documents together)
2025-01-01 10:12:59 | Shard distribution:
2025-01-01 10:12:59 |   Shard  0:    121 chunks from 8 documents
2025-01-01 10:12:59 |   Shard  1:    119 chunks from 6 documents
2025-01-01 10:12:59 |   Shard  2:    123 chunks from 5 documents
2025-01-01 10:12:59 |   Shard  3:    124 chunks from 6 documents
2025-01-01 10:15:30 | Shard 0: added=   121  updated=     0
2025-01-01 10:15:30 | Shard 1: added=   119  updated=     0
2025-01-01 10:15:30 | Shard 2: added=   123  updated=     0
2025-01-01 10:15:30 | Shard 3: added=   124  updated=     0
2025-01-01 10:15:31 | Index Statistics:
2025-01-01 10:15:31 |   Total shards: 4
2025-01-01 10:15:31 |   Total documents added: 487
2025-01-01 10:15:31 |   Total documents updated: 0
2025-01-01 10:15:31 | ✅ STEP 4 COMPLETE: Lucene index updated successfully
2025-01-01 10:15:31 | PIPELINE RUN COMPLETED SUCCESSFULLY
```

### Log Locations

**Local Mode**:
```
/Users/moudgil/output/logs/
├── rag_audit.log      # High-level events
└── rag_pipeline.log   # Detailed logs
```

**EMR Mode**:
```
s3://your-bucket/rag/logs/
├── audit/
│   ├── RAGDocNormalized_20250101_100000_1234567890.log
│   ├── RAGChunks_20250101_100301_1234567891.log
│   ├── RAGEmbeddings_20250101_100346_1234567892.log
│   └── RagLuceneIndex_20250101_101251_1234567893.log
└── pipeline/
    ├── RAGDocNormalized_20250101_100000_1234567890.log
    ├── RAGChunks_20250101_100301_1234567891.log
    ├── RAGEmbeddings_20250101_100346_1234567892.log
    └── RagLuceneIndex_20250101_101251_1234567893.log
```

---

## Running the Pipeline

### Prerequisites

**Software Requirements**:
- Scala 2.12+
- Apache Spark 3.4+
- Delta Lake 2.4+
- Apache Lucene 9.x
- SBT (for building)
- Java 11+

**For Embedding Generation** (Step 3):
- Ollama service running locally
- `mxbai-embed-large` model installed
- Install: `ollama pull mxbai-embed-large`

**For S3/EMR**:
- AWS credentials configured
- S3 bucket with appropriate permissions
- EMR cluster with Spark

### Local Mode

**1. Build the project**:
```bash
sbt clean compile assembly
```

**2. Prepare input file**:
Create `pdfs.txt` with one PDF path per line:
```
/path/to/document1.pdf
/path/to/document2.pdf
s3://bucket/path/to/document3.pdf
```

**3. Start Ollama** (for embedding generation):
```bash
ollama serve
```

**4. Run the full pipeline**:

**Option A: Run all steps in sequence**:
```bash
# Step 1: Document Normalization
spark-submit \
  --class com.rag.RAGDocNormalized \
  --master local[*] \
  target/scala-2.12/rag-assembly-1.0.jar \
  pdfs.txt

# Pipeline automatically chains to steps 2, 3, and 4
```

**Option B: Run individual steps**:

```bash
# Step 1: Document Normalization
spark-submit \
  --class com.rag.RAGDocNormalized \
  --master local[*] \
  target/scala-2.12/rag-assembly-1.0.jar \
  pdfs.txt

# Step 2: Chunking
spark-submit \
  --class com.rag.RAGChunks \
  --master local[*] \
  target/scala-2.12/rag-assembly-1.0.jar

# Step 3: Embeddings
spark-submit \
  --class com.rag.RAGEmbeddings \
  --master local[*] \
  target/scala-2.12/rag-assembly-1.0.jar

# Step 4: Indexing
spark-submit \
  --class com.rag.RagLuceneIndex \
  --master local[*] \
  target/scala-2.12/rag-assembly-1.0.jar
```

**5. Check logs**:
```bash
tail -f /Users/moudgil/output/logs/rag_audit.log
```

### EMR Mode

**1. Upload JAR and input to S3**:
```bash
# Upload JAR
aws s3 cp target/scala-2.12/rag-assembly-1.0.jar \
  s3://your-bucket/jars/

# Upload PDF list
aws s3 cp pdfs.txt s3://your-bucket/input/
```

**2. Run on EMR**:

**Option A: Full pipeline with pre-computed embeddings**:
```bash
# Pre-compute embeddings locally (on machine with Ollama)
export RUN_FULL_PIPELINE=false
spark-submit \
  --class com.rag.RAGDocNormalized \
  --master local[*] \
  target/scala-2.12/rag-assembly-1.0.jar \
  pdfs.txt

# Steps 1 and 2 run locally, step 3 generates embeddings
# Then upload Delta tables to S3

# Run indexing on EMR with skip embedding
aws emr add-steps \
  --cluster-id j-XXXXXXXXXXXXX \
  --steps Type=Spark,Name="RAG Index",\
ActionOnFailure=CONTINUE,\
Args=[--class,com.rag.RagLuceneIndex,\
--master,yarn,\
--deploy-mode,cluster,\
--conf,spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem,\
--conf,spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain,\
s3://your-bucket/jars/rag-assembly-1.0.jar]
```

**Option B: Individual steps on EMR**:

```bash
# Step 1: Document Normalization
aws emr add-steps \
  --cluster-id j-XXXXXXXXXXXXX \
  --steps Type=Spark,Name="RAG Doc Normalize",\
ActionOnFailure=CONTINUE,\
Args=[--class,com.rag.RAGDocNormalized,\
--master,yarn,\
--deploy-mode,cluster,\
s3://your-bucket/jars/rag-assembly-1.0.jar,\
s3://your-bucket/input/pdfs.txt]

# Step 2: Chunking
aws emr add-steps \
  --cluster-id j-XXXXXXXXXXXXX \
  --steps Type=Spark,Name="RAG Chunks",\
ActionOnFailure=CONTINUE,\
Args=[--class,com.rag.RAGChunks,\
--master,yarn,\
--deploy-mode,cluster,\
s3://your-bucket/jars/rag-assembly-1.0.jar]

# Step 3: Embeddings (skip on EMR)
export SKIP_EMBEDDING=true

# Step 4: Indexing
aws emr add-steps \
  --cluster-id j-XXXXXXXXXXXXX \
  --steps Type=Spark,Name="RAG Index",\
ActionOnFailure=CONTINUE,\
Args=[--class,com.rag.RagLuceneIndex,\
--master,yarn,\
--deploy-mode,cluster,\
s3://your-bucket/jars/rag-assembly-1.0.jar]
```

**3. Monitor logs**:
```bash
# View EMR logs
aws s3 ls s3://your-bucket/rag/logs/audit/ --recursive

# Download and view
aws s3 cp s3://your-bucket/rag/logs/audit/RagLuceneIndex_20250101_101251.log - | less
```

---

## Configuration

Configuration is centralized in the `Config` object. Key settings:

```scala
object Config {
  // Environment detection
  val isEMR: Boolean = sys.env.get("EMR_MODE").contains("true")
  
  // Paths (automatically switch between local and S3)
  val basePath = if (isEMR) "s3://your-bucket/rag" else "/Users/moudgil/output"
  val docNormalizedPath = s"$basePath/delta/doc_normalized"
  val chunksPath = s"$basePath/delta/chunks"
  val embeddingsPath = s"$basePath/delta/embeddings"
  val indexPath = s"$basePath/lucene_index"
  val indexedMetadataPath = s"$basePath/delta/indexed_chunks"
  val logsPath = s"$basePath/logs"
  
  // Embedding configuration
  val embedModel = "mxbai-embed-large"
  val embedVer = "v1"
  val batchSize = 50
  
  // Indexing configuration
  val numShards = 4
}
```

**Environment Variables**:
- `EMR_MODE=true`: Enable EMR mode (S3 paths)
- `RUN_FULL_PIPELINE=true`: Chain all steps automatically
- `SKIP_EMBEDDING=true`: Skip embedding generation (use pre-computed)
- `RAG_SIMILARITY=COSINE|EUCLIDEAN|DOT_PRODUCT`: Vector similarity function

---

## Incremental Updates

The pipeline is designed for **incremental processing** - only new or modified content is processed at each stage.

### How It Works

1. **Content-based hashing**:
   - Documents: `contentHash = SHA-256(text)`
   - Chunks: `chunkHash = SHA-256(chunk)`
   
2. **Change detection**:
   - Compare hashes with existing Delta tables
   - Left anti join identifies new/modified content
   
3. **Granular updates**:
   - Document level: Only changed documents re-chunked
   - Chunk level: Only changed chunks re-embedded
   - Index level: Only changed chunks re-indexed

### Example Scenarios

**Scenario 1: New PDF added**:
- Step 1: Detects new docId → extracts text
- Step 2: All chunks are new → creates chunks
- Step 3: All chunks need embeddings → generates embeddings
- Step 4: All chunks need indexing → adds to index

**Scenario 2: Existing PDF content changed**:
- Step 1: Same docId, different contentHash → extracts new text
- Step 2: Most chunks changed → re-chunks document
- Step 3: Changed chunks need new embeddings → generates embeddings
- Step 4: Changed chunks need re-indexing → updates index

**Scenario 3: No changes**:
- Step 1: All (docId, contentHash) pairs exist → skips
- Step 2: All (docId, docHash) pairs exist → skips
- Step 3: All (chunkId, chunkHash) pairs exist → skips
- Step 4: All (chunkId, chunkHash) pairs exist → skips
- Pipeline completes in seconds!

### Benefits

- **Efficiency**: Process only what changed
- **Cost savings**: Especially important for expensive embedding generation
- **Fast iterations**: Re-run pipeline frequently without waste
- **Idempotent**: Safe to re-run pipeline multiple times

---

## Project Structure

```
rag-pipeline/
├── src/main/scala/com/rag/
│   ├── RAGDocNormalized.scala    # Step 1: PDF extraction & normalization
│   ├── RAGChunks.scala           # Step 2: Document chunking
│   ├── RAGEmbeddings.scala       # Step 3: Embedding generation
│   ├── RagLuceneIndex.scala      # Step 4: Lucene index building
│   ├── AuditLogger.scala         # Logging infrastructure
│   ├── Config.scala              # Configuration management
│   ├── Chunker.scala             # Text chunking logic
│   ├── Vectors.scala             # Vector operations (L2 norm)
│   └── Ollama.scala              # Ollama API client
├── build.sbt                     # SBT build configuration
├── README.md                     # This file
└── pdfs.txt                      # Input file (PDF paths)
```

---

## Technology Stack

### Core Technologies

- **Scala 2.12**: Functional programming language
- **Apache Spark 3.4**: Distributed data processing
- **Delta Lake 2.4**: ACID transactions, time travel
- **Apache Lucene 9.x**: Full-text and vector search
- **Apache PDFBox**: PDF text extraction

### Libraries

- **Ollama**: Local LLM and embedding service
- **SBT**: Build tool
- **Hadoop AWS**: S3 integration

### Infrastructure

- **Local Development**: Spark standalone
- **Production**: AWS EMR (Elastic MapReduce)
- **Storage**: S3 for data, logs, and indexes

---

## Performance Considerations

### Optimization Tips

1. **Batch Size** (Embeddings):
   - Smaller (10-20): Lower memory, slower
   - Larger (50-100): Higher memory, faster
   - Adjust based on available RAM

2. **Number of Shards** (Indexing):
   - More shards = more parallelism
   - Recommended: 1 shard per 10-50GB of data
   - Must be set before initial index build

3. **Spark Partitions**:
   - PDF extraction: 10 partitions (I/O bound)
   - Chunking: Default (CPU bound)
   - Embeddings: 1 partition (sequential batches)
   - Indexing: numShards partitions (parallel)

4. **Delta Lake Optimization**:
   ```scala
   // Periodically optimize Delta tables
   spark.sql("OPTIMIZE rag.chunks")
   spark.sql("OPTIMIZE rag.embeddings")
   ```

### Monitoring

Check these metrics in audit logs:
- Documents per second (extraction)
- Chunks per second (chunking)
- Embeddings per second (embedding)
- Documents per shard (indexing)

---

## Troubleshooting

### Common Issues

**1. Out of Memory (Embeddings)**:
```
Solution: Reduce batchSize in Config
```

**2. Ollama Connection Failed**:
```
Check: Is Ollama service running?
Command: ollama serve
```

**3. S3 Access Denied (EMR)**:
```
Check: EMR instance role has S3 permissions
Check: Bucket policy allows access
```

**4. Delta Table Not Found**:
```
First run? Expected - tables created automatically
Verify: Config paths are correct
```

**5. Lucene Index Corruption**:
```
Delete index directory and rebuild:
rm -rf /output/lucene_index/*
Re-run Step 4
```

---

## Next Steps

After running the pipeline, you can:

1. **Query the Index**: Use Lucene's search API to query vectors
2. **Build RAG Application**: Integrate with LLM for Q&A
3. **Add More Documents**: Re-run with updated pdfs.txt
4. **Tune Performance**: Adjust batch sizes, shards based on logs
5. **Monitor Metrics**: Track processing times, success rates

---

## License

[Specify your license here]

## Contributors

[List contributors]

## Support

For issues or questions:
- Check audit logs first
- Review error messages in pipeline logs
- Verify configuration in Config.scala

---

**Happy RAG Building! 🚀**
