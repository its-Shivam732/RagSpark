package com.rag

import cats.effect._
import com.comcast.ip4s._
import io.circe.generic.auto._
import io.circe.syntax._
import org.http4s._
import org.http4s.circe._
import org.http4s.dsl.io._
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits._
import org.http4s.server.Router
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}

// API Models
case class QueryRequest(
                         query: String,
                         topK: Option[Int] = Some(5),
                         embedModel: Option[String] = Some("mxbai-embed-large"),
                         chatModel: Option[String] = Some("llama3")
                       )

case class QueryResponse(
                          query: String,
                          answer: String,
                          results: Seq[SearchResultDto],
                          processingTimeMs: Long
                        )

case class SearchResultDto(
                            docId: String,
                            chunkId: String,
                            text: String,
                            score: Float,
                            offset: Long
                          )

case class HealthResponse(
                           status: String,
                           indexPath: String,
                           shardsAvailable: Int,
                           totalDocuments: Option[Long]
                         )

case class ErrorResponse(error: String, message: String)

object RagApiServer extends IOApp {

  private val log = LoggerFactory.getLogger(getClass)

  // Configuration from environment or Config
  private val indexPath = sys.env.getOrElse("RAG_INDEX_PATH", Config.indexPath.replace("file://", ""))
  private val port = sys.env.getOrElse("RAG_API_PORT", "8080").toInt
  private val host = sys.env.getOrElse("RAG_API_HOST", "0.0.0.0")

  // JSON codecs
  implicit val queryRequestDecoder: EntityDecoder[IO, QueryRequest] = jsonOf[IO, QueryRequest]
  implicit val queryResponseEncoder: EntityEncoder[IO, QueryResponse] = jsonEncoderOf[IO, QueryResponse]
  implicit val healthResponseEncoder: EntityEncoder[IO, HealthResponse] = jsonEncoderOf[IO, HealthResponse]
  implicit val errorResponseEncoder: EntityEncoder[IO, ErrorResponse] = jsonEncoderOf[IO, ErrorResponse]
  implicit val searchResultsEncoder: EntityEncoder[IO, Seq[SearchResultDto]] = jsonEncoderOf[IO, Seq[SearchResultDto]]

  // Routes
  val ragRoutes = HttpRoutes.of[IO] {

    // POST /query - Full RAG: search + answer generation
    case req @ POST -> Root / "query" =>
      (for {
        queryReq <- req.as[QueryRequest]
        _ = log.info(s"Query received: ${queryReq.query}")

        startTime = System.currentTimeMillis()

        // Execute RAG pipeline
        answerText <- IO {
          RagSearch.answer(
            query = queryReq.query,
            indexDir = indexPath,
            embedModel = queryReq.embedModel.getOrElse("mxbai-embed-large"),
            topK = queryReq.topK.getOrElse(5),
            chatModel = queryReq.chatModel.getOrElse("llama3")
          )
        }

        // Get search results for response
        results <- IO {
          val client = new Ollama()
          try {
            val queryVec = client.embed(
              Vector(queryReq.query),
              queryReq.embedModel.getOrElse("mxbai-embed-large")
            ).head

            RagSearch.searchAllShards(
              Paths.get(indexPath),
              queryVec,
              queryReq.topK.getOrElse(5)
            ).map(r => SearchResultDto(r.docId, r.chunkId, r.text, r.score, r.offset))
          } finally {
            client.close()
          }
        }

        endTime = System.currentTimeMillis()

        response = QueryResponse(
          query = queryReq.query,
          answer = answerText,
          results = results,
          processingTimeMs = endTime - startTime
        )

        resp <- Ok(response.asJson)
      } yield resp).handleErrorWith { e =>
        log.error(s"Query failed: ${e.getMessage}", e)
        InternalServerError(ErrorResponse("query_failed", e.getMessage).asJson)
      }

    // GET /search - Search only (no answer generation)
    case GET -> Root / "search" :? QueryParam(query) +& TopKParam(topK) +& ModelParam(model) =>
      (for {
        client <- IO(new Ollama())

        queryVec <- IO {
          client.embed(Vector(query), model.getOrElse("mxbai-embed-large")).head
        }

        results <- IO {
          RagSearch.searchAllShards(
            Paths.get(indexPath),
            queryVec,
            topK.getOrElse(5)
          ).map(r => SearchResultDto(r.docId, r.chunkId, r.text, r.score, r.offset))
        }

        _ <- IO(client.close())

        resp <- Ok(results.asJson)
      } yield resp).handleErrorWith { e =>
        log.error(s"Search failed: ${e.getMessage}", e)
        InternalServerError(ErrorResponse("search_failed", e.getMessage).asJson)
      }

    // GET /health - Health check
    case GET -> Root / "health" =>
      IO {
        val indexDir = Paths.get(indexPath)
        val (shardCount, totalDocs) = if (Files.exists(indexDir)) {
          val shards = Files.list(indexDir)
            .filter(p => Files.isDirectory(p) && p.getFileName.toString.startsWith("shard_"))
            .count()
            .toInt

          // Try to count total documents across all shards
          val docCount = try {
            Some(countTotalDocuments(indexDir))
          } catch {
            case e: Exception =>
              log.warn(s"Could not count documents: ${e.getMessage}")
              None
          }

          (shards, docCount)
        } else {
          (0, None)
        }

        HealthResponse(
          status = if (shardCount > 0) "healthy" else "no_index",
          indexPath = indexPath,
          shardsAvailable = shardCount,
          totalDocuments = totalDocs
        )
      }.flatMap(health => Ok(health.asJson))
  }

  // Helper to count documents across all shards
  private def countTotalDocuments(indexDir: java.nio.file.Path): Long = {
    import scala.jdk.CollectionConverters._
    import org.apache.lucene.index.DirectoryReader
    import org.apache.lucene.store.FSDirectory

    Files.list(indexDir)
      .iterator()
      .asScala
      .filter(p => Files.isDirectory(p) && p.getFileName.toString.startsWith("shard_"))
      .map { shardPath =>
        try {
          val dir = FSDirectory.open(shardPath)
          val reader = DirectoryReader.open(dir)
          val count = reader.numDocs()
          reader.close()
          dir.close()
          count.toLong
        } catch {
          case _: Exception => 0L
        }
      }
      .sum
  }

  // Query parameter extractors
  object QueryParam extends QueryParamDecoderMatcher[String]("q")
  object TopKParam extends OptionalQueryParamDecoderMatcher[Int]("topK")
  object ModelParam extends OptionalQueryParamDecoderMatcher[String]("model")

  // HTTP app with routes
  private val httpApp = Router("/api/v1" -> ragRoutes).orNotFound

  // Server entry point
  def run(args: List[String]): IO[ExitCode] = {
    log.info("=" * 80)
    log.info("Starting RAG API Server")
    log.info("=" * 80)
    log.info(s"  Host: $host")
    log.info(s"  Port: $port")
    log.info(s"  Index: $indexPath")
    log.info("=" * 80)
    log.info("Available endpoints:")
    log.info("  POST   /api/v1/query   - Full RAG query with answer generation")
    log.info("  GET    /api/v1/search  - Search only (no answer generation)")
    log.info("  GET    /api/v1/health  - Health check")
    log.info("=" * 80)

    EmberServerBuilder
      .default[IO]
      .withHost(Host.fromString(host).getOrElse(ipv4"0.0.0.0"))
      .withPort(Port.fromInt(port).getOrElse(port"8080"))
      .withHttpApp(httpApp)
      .build
      .use { server =>
        IO {
          log.info(s"Server started at http://$host:$port")
          log.info("Press Ctrl+C to stop")
        } *> IO.never
      }
      .as(ExitCode.Success)
  }
}