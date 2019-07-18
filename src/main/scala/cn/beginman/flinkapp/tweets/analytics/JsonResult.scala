package cn.beginman.flinkapp.tweets.analytics
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write

trait JsonResult[T] {
  def +(other: T): T
  def toJson(): String = write(this)(DefaultFormats)
  def total(): Int = 0
  def key(): String = ""
}
