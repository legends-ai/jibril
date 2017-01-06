package io.asuna.jibril

import java.io.{ File, FileOutputStream }
import com.amazonaws.services.s3.transfer.TransferManager
import io.asuna.proto.enums.Region
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata
import io.asuna.asunasan.legends.AthenaLockManager
import io.asuna.proto.athena.AthenaLock
import io.asuna.proto.bacchus.BacchusData.RawMatch
import io.asuna.proto.match_filters.MatchFilters
import io.asuna.asunasan.legends.MatchSumHelpers._
import java.io.{ PipedInputStream, PipedOutputStream }
import org.xerial.snappy.{ SnappyInputStream, SnappyOutputStream }
import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.util.Try
import cats.implicits._
import cats.Apply
import io.asuna.asunasan.BaseService
import io.asuna.proto.service_jibril.JibrilGrpc
import io.asuna.proto.service_jibril.JibrilRpc
import scala.concurrent.{ ExecutionContext, Future }

class JibrilServer(args: Seq[String])(implicit ec: ExecutionContext)
    extends BaseService(args, JibrilConfigParser, JibrilGrpc.bindService) with JibrilGrpc.Jibril {

  override def consolidate(req: JibrilRpc.Sector): Future[JibrilRpc.ConsolidateResponse] = {
    // Check for the precondition that the lock exists.
    // If it doesn't, we should not run Jibril.
    val locks = new AthenaLockManager(config.service.lockBucket, req.region.name, req.version)
    for {
      lockOpt <- Future { locks.fetch() } if lockOpt.isDefined
      lock = lockOpt.get
      ct <- consolidateS3(config.service, req.region, req.version, lock.paths.toList)
    } yield JibrilRpc.ConsolidateResponse(
      count = ct,
      paths = lock.paths
    )
  }

  override def shelve(req: JibrilRpc.Sector): Future[JibrilRpc.ShelveResponse] = {
    // Check for the precondition that the lock exists.
    // If it doesn't, we should not run Jibril.
    val locks = new AthenaLockManager(config.service.lockBucket, req.region.name, req.version)
    for {
      lockOpt <- Future { locks.fetch() } if lockOpt.isDefined
      lock = lockOpt.get
      filterCt <- mergeSums(config.service, lock.filters.toList)
      consolidateCt <- consolidateS3(config.service, req.region, req.version, lock.paths.toList)
      if locks.delete().toOption.isDefined
    } yield JibrilRpc.ShelveResponse(
      consolidateCount = consolidateCt,
      paths = lock.paths,
      filterCount = filterCt
    )
  }

  /**
    * Merges the partial sums with the full sums and writes them to the correct location.
    * TODO(igm): write original to some backup location in case of failure/corruption.
    *
    * @returns the number of sums merged
    */
  def mergeSums(cfg: JibrilConfig, filtersSet: List[MatchFilters])(implicit ec: ExecutionContext): Future[Int] = {
    val db = DB.fromConfig(cfg)

    // We will iterate over every single MatchFilters and merge the sum if it exists.
    val filterFutures = filtersSet.map { filters =>
      for {
        // Fetch the sums
        partialSum <- db.partialSums.get(filters)
        matchSum <- db.matchSums.get(filters)

        // Combine them
        combo = partialSum |+| matchSum

        // Return 1 if good, 0 if bad. Used when counting number of sums merged.
        // In theory, the `None` branch should never be reached.
        count <- combo match {
          case Some(res) => db.matchSums.set(filters, res).map(_ => 1)
          case None => Future.successful(0)
        }
      } yield count
    }

    // Combine the list of futures to become a single future with a count
    filterFutures.combineAll
  }

  /**
    * Consolidates the Totsuki fragments into one large Snappy-compressed protolist.
    */
  def consolidateS3(cfg: JibrilConfig, region: Region, version: String, paths: List[String])(implicit ec: ExecutionContext): Future[Int] = {
    val s3 = new AmazonS3Client()
    val file = File.createTempFile(s"totsuki-${System.currentTimeMillis}", ".tmp")
    val fs = new FileOutputStream(file)
    val snappy = new SnappyOutputStream(fs)

    val writers = paths.map { path =>
      Future {
        Try {
          val obj = s3.getObject(cfg.fragmentsBucket, path)
          RawMatch.streamFromDelimitedInput(obj.getObjectContent).map { rawMatch =>
            rawMatch.writeDelimitedTo(fs)
          }.size
        }.toOption.getOrElse(0)
      }
    }


    for {
      // First, let's write all of the matches.
      count <- writers.combineAll

      // Here we upload the temp file to S3.
      // TransferManager will automatically make this multipart if it will improve performance.
      _ <- Future {
        val tx = new TransferManager()
        val fileName = s"${region.name}/${version}/${System.currentTimeMillis()}.protolist.snappy"
        tx.upload(cfg.matchesBucket, fileName, file).waitForCompletion()
        fs.close()
      }

      // Then we delete the fragments.
      _ <- paths.map { path =>
        Future {
          s3.deleteObject(cfg.fragmentsBucket, path)
        }
      }.sequence

      // Finally, we delete the temp file.
      _ <- Future {
        // Now we delete the tempfile since the S3 upload is complete
        file.delete()
        snappy.close()
        fs.close()
        file.delete
      }

    // We return the number of files aggregated.
    } yield count
  }

}
