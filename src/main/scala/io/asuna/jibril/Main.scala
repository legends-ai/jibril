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
import org.xerial.snappy.{ SnappyInputStream, SnappyOutputStream }
import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.util.Try


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
      case Some(lock) => run(cfg, locks, lock)
    }
  }

  def run(cfg: Config, locks: AthenaLockManager, lock: AthenaLock) = {
    implicit val ec = ExecutionContext.global
    val mergeAndConsolidate = Apply[Future].tuple2(
      mergeSums(cfg, lock.filters.toList),
      consolidateS3(cfg, lock.paths.toList)
    )

    val runEverything = for {
      (mergeCount, consolidateCount) <- mergeAndConsolidate
    } yield for {
      _ <- locks.delete()
    } yield {
      println(s"Jibril completed, merging ${mergeCount} match sums and consolidating ${consolidateCount} RawMatches over ${lock.paths.size} protolists.")
    }

    Await.result(runEverything, Duration.Inf)
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

  /**
    * Consolidates the Totsuki fragments into one large Snappy-compressed protolist.
    */
  def consolidateS3(cfg: Config, paths: List[String])(implicit ec: ExecutionContext): Future[Int] = {
    val s3 = new AmazonS3Client()

    val is = new PipedInputStream()
    val rawOs = new PipedOutputStream(is)
    val os = new SnappyOutputStream(rawOs)

    val writeToS3 = Future {
      val fileName = s"${cfg.region}/${cfg.version}/${System.currentTimeMillis()}.protolist.snappy"
      s3.putObject(cfg.matchesBucket, fileName, is, new ObjectMetadata())
    }

    val readers = paths.map { path =>
      Future {
        Try {
          val obj = s3.getObject(cfg.fragmentsBucket, path)
          val is = new SnappyInputStream(obj.getObjectContent)
          RawMatch.streamFromDelimitedInput(is).map { rawMatch =>
            rawMatch.writeDelimitedTo(os)
          }.size
        }.toOption.getOrElse(0)
      }
    }

    // Close the streams when we are done
    val readFromS3 = readers.combineAll.map { count =>
      os.close()
      rawOs.close()
      is.close()
      count
    }

    for {
      // Both write and read should finish
      (_, count) <- Apply[Future].tuple2(writeToS3, readFromS3)

      // Then we delete the fragments.
      _ <- paths.map { path =>
        Future {
          s3.deleteObject(cfg.fragmentsBucket, path)
        }
      }.sequence
    } yield count
  }

}
