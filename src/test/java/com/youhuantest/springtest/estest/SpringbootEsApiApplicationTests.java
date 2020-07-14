package com.youhuantest.springtest.estest;

import com.alibaba.fastjson.JSON;
import com.youhuantest.springtest.domain.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;

@SpringBootTest
class SpringbootEsApiApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    @Test
    void contextLoads() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("summer_002");
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(response.toString());
        System.out.println(response.index());
    }

    // 测试获取索引,判断其是否存在
    @Test
    void TestExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("summer_001");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    // 测试删除索引
    @Test
    void TestDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("summer_002");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete);
    }

    // 测试添加文档
    @Test
    void TestAddDocument() throws IOException {
        //创建对象
        User user = new User("summer", 21);
        //创建请求
        IndexRequest request = new IndexRequest("summer_002");
        //规则  /summer_02/_doc/1
        request.id("2");
        request.timeout("1s");
        //  将数据放入请求中
        request.source(JSON.toJSONString(user), XContentType.JSON);
        //客户端发送请求
        IndexResponse index = client.index(request, RequestOptions.DEFAULT);
        //查看结果
        System.out.println(index.toString());
        System.out.println(index.status());
    }

    // 获得文档的信息
    @Test
    void testGetDocument() throws IOException {
        //创建get请求
        GetRequest getRequest = new GetRequest("summer_002", "1");
        //执行命令 客户端发送，获得结果
        GetResponse documentFields = client.get(getRequest, RequestOptions.DEFAULT);
        //打印结果
        System.out.println(documentFields);
        System.out.println(documentFields.getSourceAsString());
    }

    // 更新文档的信息
    @Test
    void testUpdateDocument() throws IOException {
        //创建请求
        UpdateRequest updateRequest = new UpdateRequest("summer_002", "1");
        User user = new User("pppnut", 22);
        UpdateRequest doc = updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse update = client.update(doc, RequestOptions.DEFAULT);
        System.out.println(update);
        System.out.println(update.status());
    }

    // 删除文档记录
    @Test
    void testDeleteDocument() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("summer_002", "1");
        DeleteResponse delete = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(delete);
        System.out.println(delete.status());
    }

    // 特殊的，真的项目一般都会批量插入数据！
    @Test
    void testBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        ArrayList<User> users = new ArrayList<>();
        for (int i = 10; i < 22; i++) {
            users.add(new User("summer" + i, i));
        }
        for (int i = 0; i < users.size(); i++) {
            bulkRequest.add(
                    new IndexRequest("summer_002")
                            .source(JSON.toJSONString(users.get(i)), XContentType.JSON));
        }
        bulkRequest.timeout("10S");
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk);
        System.out.println(bulk.status());
    }

    // 查询
    // SearchRequest 搜索请求
    // SearchSourceBuilder 条件构造
    // HighlightBuilder 构建高亮
    // TermQueryBuilder 精确查询
    // MatchAllQueryBuilder
    // xxx QueryBuilder 对应我们刚才看到的命令！
    @Test
    void SearchRequest() throws IOException {
        SearchRequest searchRequest = new SearchRequest("baidu");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("amount", "200");
        // MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        SearchSourceBuilder query = sourceBuilder.query(termQueryBuilder);
        SearchRequest source = searchRequest.source(query);
        SearchResponse searchResponse = client.search(source, RequestOptions.DEFAULT);
        System.out.println(searchResponse);
        System.out.println(JSON.toJSONString(searchResponse.getHits()));
        System.out.println(searchResponse.getHits());
    }
}