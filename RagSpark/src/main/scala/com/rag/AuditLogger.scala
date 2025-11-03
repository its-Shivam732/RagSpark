package com.rag

import java.io.{File, FileWriter, PrintWriter}
import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayBuffer

object AuditLogger {
  private val logBuffer = ArrayBuffer[String]()
  private val pipelineLogBuffer = ArrayBuffer[String]()
  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  // For local mode
  private val localLogDir = if (Config.isEMR) "/tmp/rag_logs" else "/Users/moudgil/output/logs"
  private val localAuditFile = s"$localLogDir/rag_audit.log"
  private val localPipelineFile = s"$localLogDir/rag_pipeline.log"

  // Initialize local directory
  try {
    Files.createDirectories(Paths.get(localLogDir))
  } catch {
    case e: Exception =>
      System.err.println(s"Warning: Could not create log directory: ${e.getMessage}")
  }

  /**
   * Audit log - important pipeline events
   */
  def audit(message: String): Unit = {
    val timestamp = LocalDateTime.now().format(formatter)
    val logMessage = s"$timestamp | $message"

    // Always print to console
    println(logMessage)

    // Buffer for S3 upload (EMR) or immediate write (local)
    logBuffer += logMessage

    // For local, write immediately to file
    if (!Config.isEMR) {
      appendToFile(localAuditFile, logMessage)
    }
  }

  /**
   * Regular log - detailed information
   */
  def log(message: String): Unit = {
    val timestamp = LocalDateTime.now().format(formatter)
    val logMessage = s"$timestamp | $message"

    // Buffer for later
    pipelineLogBuffer += logMessage

    // For local, write immediately
    if (!Config.isEMR) {
      appendToFile(localPipelineFile, logMessage)
    }
  }

  /**
   * Write to local file (for local mode)
   */
  private def appendToFile(filename: String, message: String): Unit = {
    try {
      val fw = new FileWriter(filename, true) // append mode
      try {
        fw.write(message + "\n")
      } finally {
        fw.close()
      }
    } catch {
      case e: Exception =>
        System.err.println(s"Failed to write to log file: ${e.getMessage}")
    }
  }

  /**
   * Save all buffered logs to S3 (for EMR mode)
   * Call this at the end of each main job
   */
  def saveLogsToS3(spark: org.apache.spark.sql.SparkSession, jobName: String): Unit = {
    if (Config.isEMR && logBuffer.nonEmpty) {
      try {
        import spark.implicits._

        val timestamp = System.currentTimeMillis()
        val runId = java.time.LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))

        // Save audit log
        if (logBuffer.nonEmpty) {
          val auditDF = logBuffer.toSeq.toDF("log")
          val auditPath = s"${Config.logsPath}/audit/${jobName}_${runId}_${timestamp}.log"

          auditDF.coalesce(1)
            .write
            .mode("overwrite")
            .text(auditPath)

          println(s"✅ Audit logs saved to: $auditPath")
        }

        // Save pipeline log
        if (pipelineLogBuffer.nonEmpty) {
          val pipelineDF = pipelineLogBuffer.toSeq.toDF("log")
          val pipelinePath = s"${Config.logsPath}/pipeline/${jobName}_${runId}_${timestamp}.log"

          pipelineDF.coalesce(1)
            .write
            .mode("overwrite")
            .text(pipelinePath)

          println(s"✅ Pipeline logs saved to: $pipelinePath")
        }

      } catch {
        case e: Exception =>
          System.err.println(s"❌ Failed to save logs to S3: ${e.getMessage}")
          e.printStackTrace()
      }
    } else if (!Config.isEMR) {
      println(s"✅ Logs written to: $localLogDir")
    }
  }

  /**
   * Get summary of buffered logs (useful for debugging)
   */
  def getLogSummary: String = {
    s"""
       |Log Summary:
       |  Audit logs: ${logBuffer.size} entries
       |  Pipeline logs: ${pipelineLogBuffer.size} entries
       |  Running on: ${if (Config.isEMR) "EMR" else "Local"}
       |  Log location: ${if (Config.isEMR) Config.logsPath else localLogDir}
       |""".stripMargin
  }

  /**
   * Clear buffers (useful for testing)
   */
  def clearBuffers(): Unit = {
    logBuffer.clear()
    pipelineLogBuffer.clear()
  }
}