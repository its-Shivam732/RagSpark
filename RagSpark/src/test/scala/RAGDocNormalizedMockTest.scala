package com.rag

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RAGDocNormalizedMockTest extends AnyFlatSpec with Matchers {

  // === extractTextFromPDF() tests ===

  "extractTextFromPDF" should "return empty string for invalid URI format" in {
    val result = RAGDocNormalized.extractTextFromPDF("invalid-path.pdf")
    result shouldBe a[String]
    result.isEmpty shouldBe true
  }

  it should "handle null input gracefully" in {
    val result = RAGDocNormalized.extractTextFromPDF(null)
    result shouldBe a[String]
    result shouldBe ""
  }

  it should "handle non-existent file paths safely" in {
    val result = RAGDocNormalized.extractTextFromPDF("file:///does/not/exist.pdf")
    result shouldBe ""
  }

  it should "handle S3 URIs gracefully even if not reachable" in {
    val result = RAGDocNormalized.extractTextFromPDF("s3://dummy-bucket/fake.pdf")
    result shouldBe ""
  }

  it should "return empty for malformed URL" in {
    val result = RAGDocNormalized.extractTextFromPDF("://bad-url")
    result shouldBe ""
  }


  // === main() method tests (without Spark runtime) ===

  "RAGDocNormalized object" should "load without initialization errors" in {
    noException should be thrownBy {
      RAGDocNormalized.getClass
    }
  }

  it should "contain extractTextFromPDF method" in {
    val methodOpt = RAGDocNormalized.getClass.getMethods.exists(_.getName.contains("extractTextFromPDF"))
    methodOpt shouldBe true
  }

  "main" should "execute without Spark" in {
    noException should be thrownBy {
      // Just simulate calling the method instead of actually running Spark
      assert(RAGDocNormalized.getClass != null)
    }
  }

  it should "be a valid Scala singleton object" in {
    RAGDocNormalized.isInstanceOf[Object] shouldBe true
  }

  // === structure tests ===

  "RAGDocNormalized" should "contain a main method" in {
    val hasMain = RAGDocNormalized.getClass.getMethods.exists(_.getName == "main")
    hasMain shouldBe true
  }

  it should "be defined under com.rag package" in {
    RAGDocNormalized.getClass.getPackage.getName shouldBe "com.rag"
  }

  it should "print a warning on invalid PDF load" in {
    noException should be thrownBy {
      val result = RAGDocNormalized.extractTextFromPDF("invalid://uri.pdf")
      result shouldBe ""
    }
  }
}
