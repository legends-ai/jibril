package io.asuna.jibril

import buildinfo.BuildInfo
import scopt.OptionParser

case class Config(
  region: String = "na",
  version: String = "",
  lockBucket: String = "athena_locks",
  matchesBucket: String = "matches",
  sumsKeyspace: String = "match_sums",
  partialSumsTable: String = "partial_sums",
  fullSumsTable: String = "match_sums"
)

object Config {

  val parser = new OptionParser[Config](BuildInfo.name) {

    head(BuildInfo.name, BuildInfo.version)

    opt[String]("region")
      .text("The region we're reading matches from. Note: this is not the S3 region. Defaults to `na`.")
      .valueName("<na|euw|eune|...>")
      .action((x, c) => c.copy(region = x))

    opt[String]("version")
      .text("The match version we want to read, e.g. 6.22.1.")
      .valueName("<version>")
      .action((x, c) => c.copy(version = x)).required()

    opt[String]("lock_bucket")
      .text("The name of the S3 bucket we are reading/writing lock files from. Defaults to `athena_locks`.")
      .valueName("<bucket>")
      .action((x, c) => c.copy(lockBucket = x))

    opt[String]("matches")
      .text("The S3 bucket containing our final matches.")
      .valueName("<bucket>")
      .action((x, c) => c.copy(lockBucket = x))

    opt[String]("sums_keyspace")
      .text("The keyspace containing the sums tables.")
      .valueName("<keyspace>")
      .action((x, c) => c.copy(sumsKeyspace = x))

    opt[String]("partial_sums_table")
      .text("The output Cassandra table of Athena.")
      .valueName("<table>")
      .action((x, c) => c.copy(partialSumsTable = x))

    opt[String]("full_sums_table")
      .text("The table containing full sums.")
      .valueName("<table>")
      .action((x, c) => c.copy(fullSumsTable = x))

  }

  def mustParse(args: Array[String]): Config = {
    val result = parser.parse(args, Config())
    if (!result.isDefined) {
      // couldn't parse
      sys.exit(0)
    }
    result.get
  }

}
