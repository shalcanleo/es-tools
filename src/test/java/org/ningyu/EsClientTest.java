package org.ningyu;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class EsClientTest {
    @Test
    public void test() throws Exception {
        List<String> nodes = new ArrayList<String>();
        nodes.add("http://192.168.1.1:9200");
        EsClientConfig config = new EsClientConfig(false, "","", nodes);
        EsClient client = EsClientFactory.getClient(config);
        QueryBuilder queryBuilder = QueryBuilders.termQuery("testPara", 1);
        List<String> ids = client.getAllIdsOfQuery("testIndex", "testRecords", queryBuilder);
        System.out.println(
                client.getAllRecordsByMetaIds("testIndex", "testRecords",
                        ids, new ArrayList<String>())
        );
    }
}
