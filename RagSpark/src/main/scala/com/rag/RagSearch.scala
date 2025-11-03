package com.rag

import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.{IndexSearcher, KnnFloatVectorQuery}
import org.apache.lucene.store.FSDirectory
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._
import scala.util.Using

case class SearchResult(docId: String, chunkId: String, text: String, score: Float, offset: Long)

object RagSearch {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Search a single Lucene shard for top-k results
   */
  def searchShard(shardPath: Path, queryVec: Array[Float], k: Int): Seq[SearchResult] = {
    try {
      Using.resource(FSDirectory.open(shardPath)) { directory =>
        Using.resource(DirectoryReader.open(directory)) { reader =>
          val searcher = new IndexSearcher(reader)
          val query = new KnnFloatVectorQuery("vector", queryVec, k)
          val topDocs = searcher.search(query, k)
          val storedFields = searcher.storedFields()

          topDocs.scoreDocs.map { scoreDoc =>
            val doc = storedFields.document(scoreDoc.doc)
            SearchResult(
              docId = doc.get("doc_id"),
              chunkId = doc.get("chunk_id"),
              text = doc.get("text"),
              score = scoreDoc.score,
              offset = doc.getField("offset").numericValue().longValue()
            )
          }.toSeq
        }
      }
    } catch {
      case e: Exception =>
        log.error(s"Error searching shard ${shardPath.getFileName}: ${e.getMessage}", e)
        Seq.empty
    }
  }

  /**
   * Search all shards in parallel and merge results (fan-out/fan-in)
   */
  def searchAllShards(indexDir: Path, queryVec: Array[Float], k: Int): Seq[SearchResult] = {
    val shardDirs = Files.list(indexDir)
      .iterator()
      .asScala
      .filter(p => Files.isDirectory(p) && p.getFileName.toString.startsWith("shard_"))
      .toSeq

    if (shardDirs.isEmpty) {
      log.warn(s"No index shards found in $indexDir")
      return Seq.empty
    }

    log.info(s"Searching ${shardDirs.size} shards...")

    // Fan-out: search each shard in parallel
    val allResults = shardDirs.par.flatMap { shardPath =>
      searchShard(shardPath, queryVec, k)
    }.seq

    // Fan-in: merge results using global top-k
    allResults.sortBy(-_.score).take(k)
  }

  /**
   * Full RAG pipeline: embed query → search → pack context → generate answer
   */
  def answer(
              query: String,
              indexDir: String,
              embedModel: String = "mxbai-embed-large",
              topK: Int = 5,
              chatModel: String = "llama3"
            ): String = {



    try {
      val client = new Ollama()
      // Step 1: Embed the query
      log.info(s"Embedding query: $query")
      val queryVec = client.embed(Vector(query), embedModel).headOption

      if (queryVec.isEmpty || queryVec.get.isEmpty) {
        return "Error: Failed to generate query embedding"
      }

      // Step 2: Search Lucene index (fan-out/fan-in across shards)
      log.info(s"Searching index at $indexDir")
      val results = searchAllShards(Paths.get(indexDir), queryVec.get, topK)

      if (results.isEmpty) {
        return "No relevant documents found in the index."
      }

      log.info(s"Found ${results.size} results with scores: ${results.map(_.score).mkString(", ")}")

      // Step 3: Pack context from top results
      val context = results.zipWithIndex.map { case (r, idx) =>
        s"[Document ${idx + 1}: ${r.docId} - chunk ${r.chunkId}]:\n${r.text}"
      }.mkString("\n\n---\n\n")


      // Step 4: Generate answer using Ollama /api/chat
      log.info("Generating answer with LLM...")
      val systemMessage = new client.ChatMessage(
        role = "system",
        content = "You are a helpful assistant. Answer the user's question based ONLY on the provided context. If the answer cannot be found in the context, explicitly state that. Be concise and cite specific document numbers when referencing information."
      )

      val userMessage = new client.ChatMessage(
        role = "user",
        content = s"""Context from documents:

$context

Question: $query

Provide a clear, concise answer based only on the context above. Reference document numbers when citing information."""
      )

      def toChatMap(m: client.ChatMessage): Map[String, String] =
        Map(
          "role" -> m.role,
          "content" -> m.content
        )

      val answer = client.chat(
        Vector(
          toChatMap(systemMessage),
          toChatMap(userMessage)
        ),
        chatModel
      )
      // Include source attribution
      val sources = results.map(r => s"${r.docId} (chunk ${r.chunkId})").distinct.mkString(", ")
      s"$answer\n\n[Sources: $sources]"

    } catch {
      case e: Exception =>
        log.error(s"Error in RAG pipeline: ${e.getMessage}", e)
        s"Error generating answer: ${e.getMessage}"
    }
  }
}