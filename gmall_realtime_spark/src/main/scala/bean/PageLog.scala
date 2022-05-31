package bean

case class PageLog(mid: String,
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
                   ts: Long) {}

object PageLog {
  def apply(c: CommonInfo, p: PageInfo, ts: Long): PageLog = {
    new PageLog(c.mid, c.uid, c.ar, c.ch, c.isNew, c.md, c.os, c.vc, c.ba, p.pageId, p.lastPageId, p.pageItem, p.pageItemType, p.duringTime, p.sourceType, ts)
  }
}
