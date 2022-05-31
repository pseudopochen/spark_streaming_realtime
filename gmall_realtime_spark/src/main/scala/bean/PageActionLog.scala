package bean

case class PageActionLog(
                          mid: String,
                          user_id: String,
                          province_id: String,
                          channel: String,
                          is_new: String,
                          model: String,
                          operate_system: String,
                          version_code: String,
                          brand: String,
                          page_id: String,
                          last_page_id: String,
                          page_item: String,
                          page_item_type: String,
                          during_time: Long,
                          sourceType: String,
                          action_id: String,
                          action_item: String,
                          action_item_type: String,
                          action_ts: Long,
                          ts: Long
                        ) {

}

object PageActionLog {
  def apply(p: PageLog, a: ActionInfo): PageActionLog = {
    new PageActionLog(p.mid, p.user_id, p.province_id, p.channel, p.is_new, p.model, p.operate_system, p.version_code,
      p.brand, p.page_id, p.last_page_id, p.page_item, p.page_item_type, p.during_time, p.sourceType,
      a.actionId, a.actionItem, a.actionItemType, a.actionTs, p.ts)
  }
}
