package com.luban.monitor.mapper.impl;


import com.luban.monitor.bean.NameValue;
import com.luban.monitor.mapper.PublisherMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Repository
public class PublisherMapperImpl implements PublisherMapper {

    @Autowired
    RestHighLevelClient esClient;

    private final String indexNamePrefix = "gmall_dau_info_";

    private final String orderIndexNamePrefix = "gmall_order_wide_";

    @Override
    public Map<String, Object> searchDau(String td) {
        Map<String, Object> dauRes = new HashMap<>();
        dauRes.put("dauTotal", searchDauTotal(td));
        dauRes.put("dauTd", searchDauHr(td));
        // get yesterday's date using minusDays
        dauRes.put("dauYd", searchDauHr(LocalDate.parse(td).minusDays(1).toString()));
        return dauRes;
    }

    @Override
    public List<NameValue> searchStatsByItem(String itemName, String date, String field) {
        ArrayList<NameValue> results = new ArrayList<>();
        String indexName = orderIndexNamePrefix + date;
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        // query
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("sku_name", itemName).operator(Operator.AND);
        searchSourceBuilder.query(matchQueryBuilder);
        // group
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupby" + field).field(field).size(100);
        // sum
        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum("totalamount").field("split_total_amount");
        termsAggregationBuilder.subAggregation(sumAggregationBuilder);
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        // search
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);

        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms parsedTerms = aggregations.get("groupby" + field);
            List<? extends Terms.Bucket> buckets = parsedTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                ParsedSum parsedSum = bucket.getAggregations().get("totalamount");
                double totalamount = parsedSum.getValue();
                results.add(new NameValue(bucket.getKeyAsString(), totalamount));
            }
            return results;
        } catch (ElasticsearchStatusException ese) {
            if (ese.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + " does not exist!");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES search failed!");
        }
        return null;
    }

    @Override
    public Map<String, Object> searchDetailByItem(String date, String itemName, Integer from, Integer pageSize) {

        Map<String, Object> results = new HashMap<>();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(new String[]{"create_time", "order_price", "province_name", "sku_name", "sku_num", "total_amount", "user_age", "user_gender"}, null);
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("sku_name", itemName).operator(Operator.AND);
        searchSourceBuilder.query(matchQueryBuilder);
        searchSourceBuilder.from(from);
        searchSourceBuilder.size(pageSize);
        // highlight search terms
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("sku_name");
        searchSourceBuilder.highlighter(highlightBuilder);

        String indexName = orderIndexNamePrefix + date;
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);

        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            long total = searchResponse.getHits().getTotalHits().value;
            results.put("total", total);
            SearchHit[] hits = searchResponse.getHits().getHits();
            ArrayList<Map<String, Object>> details = new ArrayList<>();
            for (SearchHit hit : hits) {
                Map<String, Object> sourceMap = hit.getSourceAsMap();
                //get highlighted sku_name
                HighlightField highlightField = hit.getHighlightFields().get("sku_name");
                String highlightSKU = highlightField.getFragments()[0].toString();
                sourceMap.put("sku_name", highlightSKU);

                details.add(sourceMap);
            }
            results.put("detail", details);
            return results;
        } catch (ElasticsearchStatusException ese) {
            if (ese.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + " does not exist!");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES search failed!");
        }
        return null;
    }

    public Map<String, Long> searchDauHr(String td) {
        HashMap<String, Long> dauHr = new HashMap<>();
        String indexName = indexNamePrefix + td;
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // we don't need the hits details
        searchSourceBuilder.size(0);
        TermsAggregationBuilder agg = AggregationBuilders.terms("groupbyhr").field("hr").size(24);
        searchSourceBuilder.aggregation(agg);
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms parsedTerms = aggregations.get("groupbyhr");
            List<? extends Terms.Bucket> buckets = parsedTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                dauHr.put(bucket.getKeyAsString(), bucket.getDocCount());
            }
            return dauHr;
        } catch (ElasticsearchStatusException ese) {
            if (ese.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + " does not exist!");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES search failed!");
        }
        return null;

    }

    public Long searchDauTotal(String td) {
        String indexName = indexNamePrefix + td;
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            return searchResponse.getHits().getTotalHits().value;
        } catch (ElasticsearchStatusException ese) {
            if (ese.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + " does not exist!");
            }
        } catch (IOException e) {
            throw new RuntimeException("ES search failed!");
        }
        return 0L;
    }

}
