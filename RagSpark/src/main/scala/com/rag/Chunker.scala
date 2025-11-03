package com.rag

import org.slf4j.LoggerFactory

/**
 * Text chunking utility for splitting large documents into smaller, overlapping chunks.
 *
 * This is essential for RAG systems because:
 * - Embedding models have token limits
 * - Smaller chunks provide more precise retrieval
 * - Overlapping ensures context isn't lost at boundaries
 */
object Chunker {
  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Normalize whitespace in text by replacing multiple spaces with single space.
   *
   * @param s Input string with potentially irregular whitespace
   * @return Normalized string with single spaces and trimmed edges
   */
  def normalize(s: String): String = {
    // Replace all consecutive whitespace characters with a single space
    val normalized = s.replaceAll("\\s+", " ").trim
    log.debug(s"Normalized text: ${s.length} -> ${normalized.length} chars")
    normalized
  }

  /**
   * Split text into overlapping chunks with intelligent boundary detection.
   *
   * @param text The full text to be chunked
   * @param maxChars Maximum characters per chunk (default: 1200)
   * @param overlap Number of characters to overlap between chunks (default: 200)
   * @return Vector of text chunks
   */
  def split(text: String, maxChars: Int = 1200, overlap: Int = 200): Vector[String] = {
    // Validate parameters
    require(overlap < maxChars, "overlap must be less than maxChars")
    log.debug(s"Starting chunking with maxChars=$maxChars, overlap=$overlap")

    // Normalize input text
    val clean = normalize(text)
    log.debug(s"Normalized text length: ${clean.length} chars")

    // Recursive function to build chunks
    @scala.annotation.tailrec
    def chunkFrom(pos: Int, acc: Vector[String]): Vector[String] = {
      if (pos >= clean.length) {
        acc
      } else {
        // Calculate end position for this chunk
        val end = (pos + maxChars).min(clean.length)
        val slice = clean.substring(pos, end)

        // Try to find a sentence boundary (. or newline) to break at
        val cut = slice.lastIndexWhere(ch => ch == '.' || ch == '\n')

        // If we found a good breaking point in the last 40% of the chunk, use it
        // Otherwise, use the full slice (hard break)
        val piece = if (cut >= (maxChars * 0.6).toInt) {
          log.trace(s"Chunk ${acc.length}: Breaking at sentence boundary (position $cut)")
          slice.substring(0, cut + 1)
        } else {
          log.trace(s"Chunk ${acc.length}: Hard break (no sentence boundary found)")
          slice
        }

        // Move to next position with overlap, ensuring at least 1 character advance
        val nextPos = pos + (piece.length - overlap).max(1)
        chunkFrom(nextPos, acc :+ piece)
      }
    }

    // Start chunking from position 0 with empty accumulator
    val chunks = chunkFrom(0, Vector.empty[String])

    // Calculate average chunk size for logging
    val avgLength = if (chunks.nonEmpty) clean.length / chunks.length else 0
    log.info(s"Chunked text into ${chunks.length} pieces (avg length ~${avgLength} chars)")

    // Return completed vector
    chunks
  }
}