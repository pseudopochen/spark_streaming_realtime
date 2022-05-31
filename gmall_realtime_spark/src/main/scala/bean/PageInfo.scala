package bean

import com.alibaba.fastjson.JSONObject

case class PageInfo(pageId: String, pageItem: String, pageItemType: String, duringTime: Long, lastPageId: String, sourceType: String)

object PageInfo {
  def apply(pageObj: JSONObject): PageInfo = {
    val pageId = pageObj.getString("page_id")
    val pageItem = pageObj.getString("item")
    val pageItemType = pageObj.getString("item_type")
    val duringTime = pageObj.getLong("during_time")
    val lastPageId = pageObj.getString("last_page_id")
    val sourceType = pageObj.getString("source_type")
    new PageInfo(pageId, pageItem, pageItemType, duringTime, lastPageId, sourceType)
  }
}
