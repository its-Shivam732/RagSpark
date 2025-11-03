package com.rag

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ChunkerTest extends AnyFlatSpec with Matchers {

  // --- normalize() tests ---

  "normalize" should "remove extra spaces and trim text" in {
    val input = "This   is   a   test   string.  "
    val output = Chunker.normalize(input)
    output shouldBe "This is a test string."
  }

  it should "convert newlines and tabs into single spaces" in {
    val input = "Hello\tworld\nthis is\na test"
    val output = Chunker.normalize(input)
    output shouldBe "Hello world this is a test"
  }

  it should "return an empty string for whitespace-only input" in {
    Chunker.normalize("   \n\t   ") shouldBe ""
  }

  // --- split() basic behavior ---


  it should "split long text into multiple overlapping chunks" in {
    val text = "Sentence one. " + ("a" * 1500) + " Sentence two. " + ("b" * 1300) + " Sentence three."
    val chunks = Chunker.split(text, maxChars = 1200, overlap = 200)
    chunks.length should be > 1
    // Ensure overlap by checking repeated substring region
    chunks.head.takeRight(100).intersect(chunks(1).take(100)).nonEmpty shouldBe true
  }

  it should "respect overlap smaller than maxChars" in {
    val text = "A" * 2500
    val chunks = Chunker.split(text, maxChars = 1000, overlap = 200)
    chunks.length should be > 1
    all (chunks.map(_.length)) should be <= 1000
  }

  it should "throw an exception if overlap >= maxChars" in {
    an [IllegalArgumentException] should be thrownBy {
      Chunker.split("test", maxChars = 100, overlap = 100)
    }
  }

  it should "produce chunks ending on sentence boundaries when possible" in {
    val text = "This is sentence one. This is sentence two. This is sentence three."
    val chunks = Chunker.split(text, maxChars = 40, overlap = 5)
    chunks.exists(_.endsWith(".")) shouldBe true
  }

  // --- edge cases ---

  "split" should "handle empty string input" in {
    val chunks = Chunker.split("", maxChars = 500, overlap = 100)
    chunks shouldBe empty
  }

  it should "handle very small maxChars values safely" in {
    val text = "abcdefg" * 50
    val chunks = Chunker.split(text, maxChars = 20, overlap = 5)
    chunks.length should be > 1
  }

  it should "return normalized and trimmed chunks" in {
    val messyText = "This   is a \n\n   messy text example.  Next sentence here."
    val chunks = Chunker.split(messyText, maxChars = 50, overlap = 10)
    all (chunks) should not include ("\n")
    all (chunks) should not include ("  ")
  }
}
