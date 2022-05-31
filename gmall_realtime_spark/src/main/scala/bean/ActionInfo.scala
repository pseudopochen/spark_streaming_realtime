package bean

import com.alibaba.fastjson.JSONObject

case class ActionInfo(actionId: String, actionItem: String, actionItemType: String, actionTs: Long) {
}

object ActionInfo {
  def apply(actionObj: JSONObject): ActionInfo = {
    val actionId = actionObj.getString("action_id")
    val actionItem = actionObj.getString("item")
    val actionItemType = actionObj.getString("item_type")
    val actionTs = actionObj.getLong("ts")
    new ActionInfo(actionId, actionItem, actionItemType, actionTs)
  }
}
