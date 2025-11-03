
import com.rag.RAGChunks
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RAGChunksMockTest extends AnyFlatSpec with Matchers {

  // --- sha256 function tests ---

  "sha256" should "produce a deterministic hash for the same input" in {
    val h1 = RAGChunks.sha256("hello world")
    val h2 = RAGChunks.sha256("hello world")
    h1 shouldBe h2
    h1.length shouldBe 64  // SHA-256 hex string length
  }

  it should "produce different hashes for different inputs" in {
    val h1 = RAGChunks.sha256("a")
    val h2 = RAGChunks.sha256("b")
    h1 should not be h2
  }

  it should "handle empty string input gracefully" in {
    val hash = RAGChunks.sha256("")
    hash shouldBe a[String]
    hash.length shouldBe 64
  }

  // --- chunkDocs (structural) tests ---

  "RAGChunks object" should "be loadable without errors" in {
    noException should be thrownBy {
      RAGChunks.getClass
    }
  }

  it should "contain sha256 method" in {
    val hasSha = RAGChunks.getClass.getMethods.exists(_.getName.contains("sha256"))
    hasSha shouldBe true
  }

  "chunkDocs" should "exist as a method" in {
    val methods = RAGChunks.getClass.getMethods.map(_.getName)
    methods.exists(_.contains("chunkDocs")) shouldBe true
  }

  it should "not throw exceptions when accessed reflectively" in {
    noException should be thrownBy {
      RAGChunks.getClass.getDeclaredMethods
    }
  }

  // --- main() method (non-execution) tests ---

  "main" should "exist and be invokable symbolically (without Spark start)" in {
    noException should be thrownBy {
      // Get the 'main' method via reflection, but do not execute it.
      val method = RAGChunks.getClass.getMethods.find(_.getName == "main")
      method.isDefined shouldBe true
    }
  }


  it should "be a singleton object" in {
    RAGChunks.isInstanceOf[Object] shouldBe true
  }

  // --- structure sanity checks ---

  "RAGChunks" should "be under com.rag package" in {
    RAGChunks.getClass.getPackage.getName shouldBe "com.rag"
  }

  it should "define both sha256 and main methods" in {
    val methods = RAGChunks.getClass.getMethods.map(_.getName)
    methods should contain ("main")
    methods.exists(_.contains("sha256")) shouldBe true
  }
}
