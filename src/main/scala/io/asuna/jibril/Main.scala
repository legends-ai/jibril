package io.asuna.jibril

import scala.concurrent.ExecutionContext


object Main {

  def main(args: Array[String]): Unit = {
    implicit val ec = ExecutionContext.global
    new JibrilServer(args).standReady()
  }

}
