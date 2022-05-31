package bean

import com.alibaba.fastjson.{JSON, JSONObject}

case class CommonInfo(ar: String, uid: String, os: String, ch: String, isNew: String, md: String, mid: String, vc: String, ba: String) {

}

object CommonInfo {

  def apply(commonObj: JSONObject): CommonInfo = {
    val ar = commonObj.getString("ar")
    val uid = commonObj.getString("uid")
    val os = commonObj.getString("os")
    val ch = commonObj.getString("ch")
    val isNew = commonObj.getString("is_new")
    val md = commonObj.getString("md")
    val mid = commonObj.getString("mid")
    val vc = commonObj.getString("vc")
    val ba = commonObj.getString("ba")

    new CommonInfo(ar, uid, os, ch, isNew, md, mid, vc, ba)
  }

  def main(args: Array[String]): Unit = {
    val str =
      """
        |{
        |"ar": "3",
        |"uid": "36",
        |"os": "android",
        |"ch": "xiaomi",
        |"is_new": "0",
        |"md": "Xiaomi 9",
        |"mid": "mid_73",
        |"vc": "v2.1.134",
        |"ba": "Xiaomi"
        |}
        |""".stripMargin
    println(CommonInfo(JSON.parseObject(str)).ar)
  }

}
