package com.luban.monitor.service.impl;

import com.luban.monitor.bean.NameValue;
import com.luban.monitor.mapper.PublisherMapper;
import com.luban.monitor.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    PublisherMapper publisherMapper;

    /**
     * Daily-Active-User service
     *
     * @param td
     * @return
     */
    @Override
    public Map<String, Object> doDAU(String td) {
        Map<String, Object> res = publisherMapper.searchDau(td);
        return res;
    }

    /**
     * Order item statistics analysis
     *
     * @param itemName
     * @param date
     * @param t
     * @return
     */
    @Override
    public List<NameValue> doStatsByItem(String itemName, String date, String t) {
        List<NameValue> res = publisherMapper.searchStatsByItem(itemName, date, typeToField(t));
        return transformResults(res, t);
    }

    /**
     * Order item details
     *
     * @param date
     * @param itemName
     * @param pageNo
     * @param pageSize
     * @return
     */
    @Override
    public Map<String, Object> doDetailByItem(String date, String itemName, Integer pageNo, Integer pageSize) {
        int from = (pageNo - 1) * pageSize;
        Map<String, Object> res = publisherMapper.searchDetailByItem(date, itemName, from, pageSize);
        return res;
    }

    public String typeToField(String t) {
        if ("age".equals(t)) {
            return "user_age";
        } else if ("gender".equals(t)) {
            return "user_gender";
        } else {
            return null;
        }
    }

    public List<NameValue> transformResults(List<NameValue> searchResults, String t) {

        if (searchResults == null || searchResults.size() == 0) {
            return searchResults;
        }

        if ("gender".equals(t)) {
            for (NameValue searchResult : searchResults) {
                if ("F".equals(searchResult.getName())) {
                    searchResult.setName("女");
                } else {
                    searchResult.setName("男");
                }
            }
        } else if ("age".equals(t)) {
            double total20 = 0;
            double total20to29 = 0;
            double total30 = 0;
            for (NameValue searchResult : searchResults) {
                int age = Integer.parseInt(searchResult.getName().toString());
                double amount = Double.parseDouble(searchResult.getValue().toString());
                if (age < 20) {
                    total20 += amount;
                } else if (age <= 29) {
                    total20to29 += amount;
                } else {
                    total30 += amount;
                }
            }
            searchResults.clear();
            searchResults.add(new NameValue("20岁以下", total20));
            searchResults.add(new NameValue("20到29岁", total20to29));
            searchResults.add(new NameValue("30岁以上", total30));
        }
        return searchResults;
    }
}
