package utils

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.mutable.ListBuffer


/**
 * Read/write to ElasticSearch
 */
object EsUtils {

  val esClient: RestHighLevelClient = build()

  /**
   * create es client
   *
   * @return
   */
  def build(): RestHighLevelClient = {
    val restClientBuilder = RestClient.builder(new HttpHost(PropsUtil(Config.ES_HOST), PropsUtil(Config.ES_PORT).toInt))
    new RestHighLevelClient(restClientBuilder)
  }

  /**
   * close es client
   */
  def close(): Unit = {
    if (esClient != null)
      esClient.close()
  }

  /**
   * 1. bulk write into the same index name
   * 2. idempotent write
   */
  def bulkSave(indexName: String, docs: List[(String, AnyRef)]): Unit = {
    val bulkReq = new BulkRequest(indexName)
    for ((docId, docObj) <- docs) {
      val request = new IndexRequest()
      request.id(docId)
      val docStr = JSON.toJSONString(docObj, new SerializeConfig(true))
      request.source(docStr, XContentType.JSON)
      bulkReq.add(request)
    }
    esClient.bulk(bulkReq, RequestOptions.DEFAULT)
  }

  /**
   * check if indexName exists
   *
   * @param indexName
   * @return
   */
  def checkExist(indexName: String): Boolean = {
    val getIndexRequest = new GetIndexRequest(indexName)
    esClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT)
  }

  /**
   * Search indexName for the specified fieldName
   *
   * @param indexName
   * @param fieldName
   * @return
   */

  def searchField(indexName: String, fieldName: String): List[String] = {
    if (!checkExist(indexName)) {
      return null
    }

    val mids = ListBuffer[String]()

    val searchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder.fetchSource(fieldName, null).size(100000) // the default size is 10, not enough for our app, in ES, change PUT /_settings
    //    {
    //      "index.max_result_window":"500000"
    //    }
    val searchRequest = new SearchRequest(indexName)
    searchRequest.source(searchSourceBuilder)
    val searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT)

    val hits = searchResponse.getHits.getHits
    for (hit <- hits) {
      val sourceMap = hit.getSourceAsMap
      val mid = sourceMap.get(fieldName).toString
      mids.append(mid)
    }
    mids.toList
  }

  def main(args: Array[String]): Unit = {
    println(searchField("gmall_dau_info_2022-03-30", "mid"))
  }

}
