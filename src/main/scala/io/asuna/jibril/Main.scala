package io.asuna.jibril

import cats.implicits._
import io.asuna.asunasan.legends.AthenaLockManager
import io.asuna.proto.athena.AthenaLock
import io.asuna.proto.match_filters.MatchFilters
import io.asuna.asunasan.legends.MatchSumHelpers._
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
    filtersSet.map { filters =>
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
    }.combineAll
  }


}
