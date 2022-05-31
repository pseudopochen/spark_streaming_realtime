package bean

import com.alibaba.fastjson.JSONObject

case class StartLog(
                     mid: String,
                     user_id: String,
                     province_id: String,
                     channel: String,
                     is_new: String,
                     model: String,
                     operate_system: String,
                     version_code: String,
                     brand: String,
                     entry: String,
                     open_ad_id: String,
                     loading_time_ms: Long,
                     open_ad_ms: Long,
                     open_ad_skip_ms: Long,
                     ts: Long
                   ) {

}

object StartLog {
  def apply(c: CommonInfo, startObj: JSONObject, ts: Long): StartLog = {
    val entry = startObj.getString("entry")
    val loadingTime = startObj.getLong("loading_time")
    val openAdId = startObj.getString("open_ad_id")
    val openAdMs = startObj.getLong("open_ad_ms")
    val openAdSkipMs = startObj.getLong("open_ad_skip_ms")
    new StartLog(c.mid, c.uid, c.ar, c.ch, c.isNew, c.md, c.os, c.vc, c.ba, entry, openAdId, loadingTime, openAdMs, openAdSkipMs, ts)

  }
}
