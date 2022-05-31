package bean

import com.alibaba.fastjson.JSONObject


case class DisplayInfo(displayType: String, displayItem: String, displayItemType: String, posId: String, order: String) {
}

object DisplayInfo {
  def apply(displayObj: JSONObject): DisplayInfo = {
    val displayType = displayObj.getString("display_type")
    val displayItem = displayObj.getString("item")
    val displayItemType = displayObj.getString("item_type")
    val posId = displayObj.getString("pos_id")
    val order = displayObj.getString("order")
    new DisplayInfo(displayType, displayItem, displayItemType, posId, order)
  }
}