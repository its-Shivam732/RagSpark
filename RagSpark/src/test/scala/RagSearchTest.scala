import com.rag.RagSearch
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}

class RagSearchTest extends AnyFunSuite with Matchers {

  test("searchShard should return empty results for non-existent directory") {
    val results = RagSearch.searchShard(Paths.get("nonexistent_shard"), Array(0.1f, 0.2f), 3)
    results shouldBe empty
  }

  test("searchAllShards should warn and return empty when no shards exist") {
    val tmpDir = Files.createTempDirectory("empty_index")
    val results = RagSearch.searchAllShards(tmpDir, Array(0.1f, 0.2f), 3)
    results shouldBe empty
  }


  test("answer should handle empty embedding gracefully") {
    // Simulate an empty embedding model (expecting an error message)
    val result = RagSearch.answer(
      query = "",
      indexDir = "nonexistent_index",
      embedModel = "mxbai-embed-large",
      chatModel = "llama3"
    )
    // It might return either "Error" or "No relevant documents" depending on embed() behavior
    result should (
      include("Error") or include("No relevant documents")
      )
  }

  test("searchAllShards should handle directory with non-shard subdirectories") {
    val tmpDir = Files.createTempDirectory("mixed_index")
    Files.createDirectory(tmpDir.resolve("not_a_shard"))
    val results = RagSearch.searchAllShards(tmpDir, Array(0.1f, 0.2f), 3)
    results shouldBe empty
  }

  test("answer should not throw exceptions for simple valid inputs") {
    noException should be thrownBy {
      RagSearch.answer(
        query = "Explain Lucene search",
        indexDir = "nonexistent_index",
        embedModel = "mxbai-embed-large",
        chatModel = "llama3"
      )
    }
  }
}
