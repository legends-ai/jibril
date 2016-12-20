# jibril

Bookkeeper for Athena results.

* Merges partial MatchSums into the full MatchSum
* Moves S3 files from `bacchus-out` to `matches`
  * Merges these Parquet files
* Deletes `athena.lock`
