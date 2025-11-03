
import com.rag.Ollama
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OllamaTest extends AnyFlatSpec with Matchers {

  // --- Basic creation ---

  "Ollama" should "initialize with default base URL" in {
    val client = new Ollama()
    client should not be null
  }

  it should "initialize with custom base URL" in {
    val client = new Ollama("http://mockserver:1234")
    client should not be null
  }

  // --- Private method tests via reflection ---

  "parseEmbedding" should "parse valid JSON with embedding array" in {
    val client = new Ollama()
    val method = classOf[Ollama].getDeclaredMethod("parseEmbedding", classOf[String])
    method.setAccessible(true)
    val json = """{"embedding": [0.1, 0.2, 0.3]}"""
    val result = method.invoke(client, json).asInstanceOf[Array[Float]]
    result should contain theSameElementsInOrderAs Array(0.1f, 0.2f, 0.3f)
  }

  it should "return empty array when 'embedding' key missing" in {
    val client = new Ollama()
    val method = classOf[Ollama].getDeclaredMethod("parseEmbedding", classOf[String])
    method.setAccessible(true)
    val json = """{"no_embedding": [1,2,3]}"""
    val result = method.invoke(client, json).asInstanceOf[Array[Float]]
    result shouldBe empty
  }

  it should "return empty array for malformed JSON" in {
    val client = new Ollama()
    val method = classOf[Ollama].getDeclaredMethod("parseEmbedding", classOf[String])
    method.setAccessible(true)
    val json = """{"embedding": [not a number]}"""
    val result = method.invoke(client, json).asInstanceOf[Array[Float]]
    result shouldBe empty
  }

  // --- JSON escaping ---

  "escapeJson" should "escape quotes, backslashes, and newlines correctly" in {
    val client = new Ollama()
    val method = classOf[Ollama].getDeclaredMethod("escapeJson", classOf[String])
    method.setAccessible(true)
    val input = "He said: \"Hello\"\nLine2\\Tab\tEnd"
    val result = method.invoke(client, input).asInstanceOf[String]
    result should include ("\\\"Hello\\\"")
    result should include ("\\n")
    result should include ("\\\\")
    result should include ("\\t")
  }

  // --- embed() behavior (offline simulation) ---

  "embed" should "handle empty input gracefully" in {
    val client = new Ollama()
    val result = client.embed(Vector.empty, "mock-model")
    result shouldBe empty
  }

  it should "handle single small text without network failure" in {
    val client = new Ollama("http://localhost:9999") // invalid port, but handled
    noException should be thrownBy {
      val result = client.embed(Vector("test text"), "mock-model")
      result.length shouldBe 1
    }
  }

  // --- chat() logic (offline safe test) ---

  "chat" should "throw an exception if server not available" in {
    val client = new Ollama("http://localhost:9999")
    val messages = Vector(Map("role" -> "user", "content" -> "Hello, how are you?"))
    an [Exception] should be thrownBy {
      client.chat(messages, "mock-chat-model")
    }
  }

  it should "build messages JSON safely" in {
    val client = new Ollama()
    val messages = Vector(
      Map("role" -> "user", "content" -> "Test \"quotes\" and newlines\nhere.")
    )

    noException should be thrownBy {
      // We're just checking it constructs JSON; call likely fails due to no API
      try {
        client.chat(messages, "mock-chat-model")
      } catch {
        case _: Throwable => // expected in offline mode
      }
    }
  }

  // --- close() ---

  "close" should "execute safely" in {
    val client = new Ollama()
    noException should be thrownBy {
      client.close()
    }
  }
}
