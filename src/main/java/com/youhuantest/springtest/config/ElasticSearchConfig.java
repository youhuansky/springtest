package com.youhuantest.springtest.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticSearchConfig {

    @Bean
    public RestHighLevelClient restHighLevelClient() {
        return new RestHighLevelClient(
                RestClient.builder(new HttpHost("hadoop101", 9200, "http")
                        , new HttpHost("hadoop102", 9200, "http")
                        , new HttpHost("hadoop103", 9200, "http")));
    }

}
