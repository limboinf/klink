package cn.beginman.flinkapp.tweets.analytics

case class CustomCount(typeName: String,
                       word: String,
                       timestamp: Long,
                       count: Int,
                       mapper: Option[String => String] = None) extends JsonResult[CustomCount] {

  override def +(other: CustomCount): CustomCount = this.copy(count = count + other.count)

  override def toJson(): String = {
    val f =
      if (mapper.nonEmpty) {
        mapper.get.apply(word)
      } else {
        word
      }

    s"""{
       |  "count": ${count},
       |  "$typeName": "${f}",
       |  "timestamp": ${timestamp}
       |}
     """.stripMargin
  }

  override def total(): Int = count

  override def key(): String = word
}
