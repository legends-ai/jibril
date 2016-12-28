package io.asuna.jibril

import com.websudos.phantom.connectors.{ ContactPoints, KeySpaceDef }
import com.websudos.phantom.database.Database
import io.asuna.asunasan.legends.models.ConcreteSumsModel

object DB {

  def fromConfig(cfg: JibrilConfig): DB = {
    new DB(ContactPoints(cfg.cassandraHosts).keySpace(cfg.sumsKeyspace), cfg)
  }

}

class DB(val keyspace: KeySpaceDef, cfg: JibrilConfig) extends Database[DB](keyspace) {

  object partialSums extends ConcreteSumsModel(cfg.partialSumsTable) with keyspace.Connector

  object matchSums extends ConcreteSumsModel(cfg.fullSumsTable) with keyspace.Connector

}
