package io.asuna.jibril

import cats.Apply
import cats.implicits._
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata
import io.asuna.asunasan.legends.AthenaLockManager
import io.asuna.proto.athena.AthenaLock
import io.asuna.proto.bacchus.BacchusData.RawMatch
import io.asuna.proto.match_filters.MatchFilters
import io.asuna.asunasan.legends.MatchSumHelpers._
import java.io.{ PipedInputStream, PipedOutputStream }
import org.xerial.snappy.SnappyInputStream
import scala.concurrent.{ ExecutionContext, Future }


object Main {

  def main(args: Array[String]): Unit = {
    val cfg = Config.mustParse(args)

    // Check for the precondition that the lock exists.
    // If it doesn't, we should not run Jibril.
    val locks = new AthenaLockManager(cfg.lockBucket, cfg.region, cfg.version)
    locks.fetch() match {
      case None => {
        println("The lock doesn't exist.")
        sys.exit(1)
      }
      case Some(lock) => run(cfg, lock)
    }
  }

  def run(cfg: Config, lock: AthenaLock) = {
    implicit val ec = ExecutionContext.global
    mergeSums(cfg, lock.filters.toList)
  }

  /**
    * Merges the partial sums with the full sums and writes them to the correct location.
    * TODO(igm): write original to some backup location in case of failure/corruption.
    *
    * @returns the number of sums merged
    */
  def mergeSums(cfg: Config, filtersSet: List[MatchFilters])(implicit ec: ExecutionContext): Future[Int] = {
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

  def consolidateS3(cfg: Config, paths: List[String])(implicit ec: ExecutionContext): Future[Unit] = {
    val s3 = new AmazonS3Client()

    val is = new PipedInputStream()
    val os = new PipedOutputStream(is)

    val writeToS3 = Future {
      val fileName = s"${cfg.region}/${cfg.version}/${System.currentTimeMillis()}.protolist.snappy"
      s3.putObject(cfg.matchesBucket, fileName, is, new ObjectMetadata())
    }

    val readers = paths.map { path =>
      Future {
        val obj = s3.getObject(cfg.lockBucket, path)
        val is = new SnappyInputStream(obj.getObjectContent)
        RawMatch.streamFromDelimitedInput(is).map { rawMatch =>
          rawMatch.writeDelimitedTo(os)
        }
      }
    }

    // Close the streams when we are done
    val readFromS3 = readers.sequence.map { _ =>
      os.close()
      is.close()
    }

    // Both write and read should finish at the same time
    Apply[Future].tuple2(writeToS3, readFromS3).map { _ => () }
  }

}
