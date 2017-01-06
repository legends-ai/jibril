package io.asuna.jibril

import buildinfo.BuildInfo
import io.asuna.asunasan.ConfigParser

case class JibrilConfig(
  lockBucket: String = "athena_locks",
  matchesBucket: String = "matches-dev",
  fragmentsBucket: String = "totsuki-fragments-dev",
  sumsKeyspace: String = "match_sums_dev",
  partialSumsTable: String = "partial_sums",
  fullSumsTable: String = "match_sums",
  cassandraHosts: Seq[String] = Seq("localhost")
)

object JibrilConfigParser extends ConfigParser[JibrilConfig](
  myService = BuildInfo.name,
  version = BuildInfo.version,
  port = 30493,
  healthPort = 30494,
  initial = JibrilConfig()
) {
    opt[String]("lock_bucket")
      .text("The name of the S3 bucket we are reading/writing lock files from. Defaults to `athena_locks`.")
      .valueName("<bucket>")
      .action((x, c) => c.copy(service = c.service.copy(lockBucket = x)))

    opt[String]("matches_bucket")
      .text("The S3 bucket containing our final matches.")
      .valueName("<bucket>")
      .action((x, c) => c.copy(service = c.service.copy(matchesBucket = x)))

    opt[String]("fragments_bucket")
      .text("The S3 bucket containing the Totsuki fragments.")
      .valueName("<bucket>")
      .action((x, c) => c.copy(service = c.service.copy(fragmentsBucket = x)))

    opt[String]("sums_keyspace")
      .text("The keyspace containing the sums tables.")
      .valueName("<keyspace>")
      .action((x, c) => c.copy(service = c.service.copy(sumsKeyspace = x)))

    opt[String]("partial_sums_table")
      .text("The output Cassandra table of Athena.")
      .valueName("<table>")
      .action((x, c) => c.copy(service = c.service.copy(partialSumsTable = x)))

    opt[String]("full_sums_table")
      .text("The table containing full sums.")
      .valueName("<table>")
      .action((x, c) => c.copy(service = c.service.copy(fullSumsTable = x)))

    opt[Seq[String]]("cassandra_hosts").valueName("<node1>,<node2>...<node_n>")
      .action((x, c) => c.copy(service = c.service.copy(cassandraHosts = x)))
      .text("List of Cassandra hosts to connect to.")
}
