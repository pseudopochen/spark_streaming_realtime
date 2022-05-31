package bean

case class PageDisplayLog(mid: String,
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
                          display_type: String,
                          display_item: String,
                          display_item_type: String,
                          display_order: String,
                          display_pos_id: String,
                          ts: Long) {}

object PageDisplayLog {
  def apply(p: PageLog, d: DisplayInfo): PageDisplayLog = {
    new PageDisplayLog(p.mid, p.user_id, p.province_id, p.channel, p.is_new, p.model, p.operate_system,
      p.version_code, p.brand, p.page_id, p.last_page_id, p.page_item, p.page_item_type, p.during_time, p.sourceType,
      d.displayType, d.displayItem, d.displayItemType, d.order, d.posId, p.ts)
  }
}
