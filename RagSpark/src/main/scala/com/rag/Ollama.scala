package com.rag

import io.circe.Encoder
import org.http4s.circe
import scalaj.http._
import org.slf4j.LoggerFactory
import org.slf4j.LoggerFactory

import scala.util.matching.Regex
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
/**
 * Ollama API client for embedding and chat completion.
 *
 * Uses manual JSON parsing to avoid dependency conflicts with Spark.
 *
 * Default Ollama endpoint: http://127.0.0.1:11434
 * Can be overridden with OLLAMA_HOST
 */
class Ollama(base: String = sys.env.getOrElse("OLLAMA_HOST", "http://127.0.0.1:11434")) {

  private val log = LoggerFactory.getLogger(getClass)

  // API endpoints
  private val eurl = s"$base/api/embeddings"
  private val curl = s"$base/api/chat"

  log.info(s"Initialized Ollama client with base URL: $base")

  /**
   * Simple JSON array parser for embedding responses.
   * Extracts float array from JSON like: {"embedding": [0.1, 0.2, 0.3]}
   */
  private def parseEmbedding(json: String): Array[Float] = {
    try {
      // Find "embedding": [ ... ] pattern
      val pattern = """"embedding"\s*:\s*\[([\d\.,\s\-eE]+)\]""".r
      pattern.findFirstMatchIn(json) match {
        case Some(m) =>
          val numbersStr = m.group(1)
          numbersStr.split(",").map(_.trim.toFloat)
        case None =>
          log.warn(s"Could not find 'embedding' field in JSON response")
          Array.empty[Float]
      }
    } catch {
      case e: Exception =>
        log.error(s"Error parsing embedding JSON: ${e.getMessage}")
        Array.empty[Float]
    }
  }

  /**
   * Escape text for JSON string value.
   */
  private def escapeJson(text: String): String = {
    text
      .replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t")
  }

  /**
   * Generate embeddings for a vector of texts.
   *
   * @param texts Vector of texts to embed
   * @param model Embedding model name (e.g., "mxbai-embed-large")
   * @return Vector of embedding arrays (one per input text)
   */
  def embed(texts: Vector[String], model: String): Vector[Array[Float]] = {
    log.info(s"Generating embeddings for ${texts.size} texts using model: $model")

    val results = texts.zipWithIndex.map { case (text, idx) =>
      log.debug(s"Embedding text ${idx + 1}/${texts.size}: ${text.take(50)}...")

      try {
        // Create simple JSON request
        val requestBody = s"""{"model": "$model", "prompt": "${escapeJson(text)}"}"""

        // Send HTTP request
        val response = Http(eurl)
          .postData(requestBody)
          .header("Content-Type", "application/json")
          .header("Accept", "application/json")
          .timeout(connTimeoutMs = 5000, readTimeoutMs = 60000)
          .asString

        if (response.isSuccess) {
          val embedding = parseEmbedding(response.body)

          if (embedding.nonEmpty) {
            log.debug(s"Successfully generated embedding for text ${idx + 1}: ${embedding.length} dimensions")
          } else {
            log.warn(s"Empty embedding for text ${idx + 1}")
          }

          embedding
        } else {
          log.error(s"Ollama API error for text ${idx + 1}: ${response.code} - ${response.body.take(200)}")
          Array.empty[Float]
        }

      } catch {
        case e: Exception =>
          log.error(s"Exception during embedding for text ${idx + 1}: ${e.getMessage}", e)
          Array.empty[Float]
      }
    }

    val successCount = results.count(_.nonEmpty)
    log.info(s"Completed embedding: $successCount/${texts.size} successful")

    results
  }

  final case class ChatMessage(role: String, content: String)
  object ChatMessage {
    implicit val chatMessageEncoder: Encoder[ChatMessage] = deriveEncoder
  }

  /**
   * Generate chat completion using conversation history.
   *
   * @param messages Conversation messages as maps with "role" and "content"
   * @param model Chat model name (e.g., "llama3.2")
   * @return Generated response text
   */
  def chat(messages: Vector[Map[String, String]], model: String): String = {
    log.info(s"Generating chat completion using model: $model")
    log.debug(s"Message count: ${messages.size}")

    try {
      // Build messages JSON array manually
      val messagesJson = messages.map { msg =>
        val role = msg.getOrElse("role", "user")
        val content = escapeJson(msg.getOrElse("content", ""))
        s"""{"role": "$role", "content": "$content"}"""
      }.mkString("[", ",", "]")

      val requestBody = s"""{"model": "$model", "messages": $messagesJson, "stream": false}"""

      val response = Http(curl)
        .postData(requestBody)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .timeout(connTimeoutMs = 5000, readTimeoutMs = 120000)
        .asString

      if (response.isSuccess) {
        // Extract content from response: {"message": {"content": "..."}}
        val contentPattern = """"content"\s*:\s*"((?:[^"\\]|\\.)*)"""".r
        contentPattern.findFirstMatchIn(response.body) match {
          case Some(m) =>
            val content = m.group(1)
              .replace("\\n", "\n")
              .replace("\\\"", "\"")
              .replace("\\\\", "\\")

            log.info("Chat completion generated successfully")
            log.debug(s"Response length: ${content.length} chars")
            content

          case None =>
            throw new RuntimeException(s"Could not parse chat response: ${response.body.take(200)}")
        }
      } else {
        throw new RuntimeException(s"Chat API error: ${response.code} - ${response.body.take(200)}")
      }

    } catch {
      case e: Exception =>
        log.error(s"Exception during chat completion: ${e.getMessage}", e)
        throw e
    }
  }

  /**
   * Close the HTTP client (no-op for scalaj-http).
   */
  def close(): Unit = {
    log.info("Ollama client closed")
  }
}