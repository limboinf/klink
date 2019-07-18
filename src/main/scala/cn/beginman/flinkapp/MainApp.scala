package cn.beginman.flinkapp

import org.slf4j.{Logger, LoggerFactory}

object MainApp {

  lazy val log: Logger = LoggerFactory.getLogger(MainApp.getClass)

  def main(args: Array[String]): Unit = {
    log.info("Starting app ...")
  }

}
