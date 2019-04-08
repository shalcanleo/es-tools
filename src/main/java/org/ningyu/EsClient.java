package org.ningyu;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.*;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;
import io.searchbox.params.Parameters;
import lombok.Data;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Data
public class EsClient {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private JestClient client;

    public EsClient(JestClient client) {
        this.client = client;
    }

    /*
     * DDL Index CRUD
     * */

    /**
     * Check index exist
     *
     * @param indexName
     * @return
     * @throws RuntimeException, IOException
     */
    public boolean indexExist(String indexName) throws RuntimeException, IOException {
        if (this.client == null) {
            throw new RuntimeException("esClient has not been set.");
        }
        return client.execute(new IndicesExists.Builder(indexName).build()).isSucceeded();
    }

    /**
     * Create Index
     *
     * @param indexName
     * @param shards
     * @param replicas
     * @return
     * @throws RuntimeException, IOException
     */
    public boolean createIndex(String indexName, int shards, int replicas) throws RuntimeException, IOException {
//        if (this.client == null) {
//            throw new RuntimeException("esClient has not been set.");
//        }
//        if (indexExist(indexName)) {
//            return true;
//        }
//        Settings.Builder settingsBuilder = Settings.builder();
//        settingsBuilder.put("number_of_shards", shards).put("number_of_replicas", replicas);
//        JestResult result = client.execute(new CreateIndex.Builder(indexName).settings(settingsBuilder.build().toString()).build());
//        if (!result.isSucceeded()) {
//            logger.error("Create Index Fail. Reason: " + result.getErrorMessage());
//        }
//        return result.isSucceeded();
        return true;
    }

    /**
     * Delete Index
     *
     * @param indexName
     * @return
     * @throws Exception
     */
    public boolean deleteIndex(String indexName) throws RuntimeException, IOException {
//        if (this.client == null) {
//            throw new RuntimeException("esClient has not been set.");
//        }
//        return client.execute(new DeleteIndex.Builder(indexName).build()).isSucceeded();
        return true;
    }


    /**
     * Get Index's Mapping
     *
     * @param indexName
     * @param typeName
     * @return
     * @throws Exception
     */
    public List<String> getMappingName(String indexName, String typeName) throws Exception {
        List<String> res = new ArrayList<String>();
        if (indexExist(indexName)){
            GetMapping getMapping = new GetMapping.Builder().addIndex(indexName).addType(typeName).build();
            JestResult result = client.execute(getMapping);
            if (result.getErrorMessage() != null || !result.isSucceeded()) {
                logger.error("Error when get mapping: " + indexName + " type: " + typeName + ": " + result.getErrorMessage());
            } else {

                JsonObject jb = result.getJsonObject().
                        getAsJsonObject(indexName).
                        getAsJsonObject("mappings").
                        getAsJsonObject(typeName).getAsJsonObject("properties");
                for (Map.Entry<String, JsonElement> entry : jb.entrySet()) {
                    res.add(entry.getKey());
                }
            }
        }
        return res;
    }

    /**
     * Add Mappings for index
     *
     * @param indexName
     * @param typeName
     * @param mappings
     * @throws Exception
     */
    public void addMappingsInIndex(String indexName, String typeName, List<Mapping> mappings) throws Exception {
//        List<String> existMapping = getMappingName(indexName, typeName);
//        String source = "{\"" + typeName + "\" : { \"properties\":{";
//        List<String> mappingString = new ArrayList<String>();
//        for (Mapping map : mappings) {
//            if (!existMapping.contains(map.getParaName())) {
//                mappingString.add(map.toString());
//            }
//        }
//        if (!mappingString.isEmpty()) source += StringUtils.join(mappingString.toArray(), ",");
//        source += "}}}";
//        PutMapping putMapping = new PutMapping.Builder(indexName, typeName, source).build();
//        JestResult res = client.execute(putMapping);
//        if (res.getErrorMessage() != null) {
//            logger.error("Add Mapping Error: " + res.getErrorMessage());
//        }
    }

    // CRUD Documents
    /**
     * Add document with id.
     *
     * @param indexName
     * @param typeName
     * @param keyValues
     * @param id
     * @return
     * @throws Exception
     */
    public String addDocument(String indexName, String typeName, Map<String, Object> keyValues, String id)
            throws Exception {
        Index index = null;
        if (id == null) {
            index = new Index.Builder(keyValues).index(indexName).type(typeName).build();
        } else {
            index = new Index.Builder(keyValues).index(indexName).type(typeName).id(id).build();
        }

        DocumentResult res = client.execute(index);
        if (!res.isSucceeded()) {
            logger.error("Create Document Error. Reason: " + res.getErrorMessage() + "\n body:" + keyValues);
        }
        return res.getId();
    }

    /**
     * Add document use es default id.
     * @param indexName
     * @param typeName
     * @param keyValues
     * @return
     * @throws Exception
     */
    public String addDocWithoutId(String indexName, String typeName, Map<String, Object> keyValues) throws Exception{
        return addDocument(indexName, typeName, keyValues, null);
    }


    // Query
    /**
     * Get all _id of query.
     * @param indexName
     * @param typeName
     * @param queryBuilder
     * @return
     * @throws Exception
     */
    public List<String> getAllIdsOfQuery(String indexName, String typeName, QueryBuilder queryBuilder) {
        List<String> res = new ArrayList<String>();
        try {
            List<Map<String, Object>> ress = getAllRecordsValueOfQuery(indexName, typeName, queryBuilder, new ArrayList<String>());
            for (Map<String, Object> map : ress) {
                if (map.containsKey("es_metadata_id")) res.add(map.get("es_metadata_id").toString());
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            logger.error("Error when get query ids. " + e.getMessage(), e);
        }
        return res;
    }

    // Query
    /**
     * Get all records by meta ids.
     * @param indexName
     * @param typeName
     * @param ids
     * @param sourceList
     * @return
     * @throws Exception
     */
    public Map<String, Map<String, Object>> getAllRecordsByMetaIds(String indexName, String typeName, List<String> ids, List<String> sourceList) throws IOException {
        Map<String, Map<String, Object>> res = new HashMap<String, Map<String, Object>>();
        List<Doc> docs = new ArrayList<Doc>();
        for (String id : ids) {
            Doc doc = new Doc(indexName, typeName, id);
            if (!sourceList.isEmpty()) doc.setSource(sourceList);
            docs.add(doc);
        }

        Action action = new MultiGet.Builder.ByDoc(docs).build();
        JestResult result = client.execute(action);
        if (!result.isSucceeded()) {
            logger.error("Error when get data. " + result.getErrorMessage());
        } else {
            try {
                List<HashMap> results = result.getSourceAsObjectList(HashMap.class);
                for (HashMap resultMap : results) {
                    String id = resultMap.get("es_metadata_id").toString();
                    resultMap.remove("es_metadata_id");
                    resultMap.remove("es_metadata_version");
                    if (!resultMap.isEmpty()) res.put(id, resultMap);
                }
            } catch (Exception e) {
                logger.error("Get Objects From Source Error: " + e.getMessage(), e);
            }
        }
        return res;
    }


    public List<Map<String, Object>> getAllRecordsValueOfQuery(String indexName, String typeName, QueryBuilder queryBuilder, List<String> sourceList) throws IOException {
        Search.Builder searchBuild = new Search.Builder(new SearchSourceBuilder().query(queryBuilder).toString()).
                addIndex(indexName).addType(typeName);
        if (sourceList != null && !sourceList.isEmpty()) {
            for (String para : sourceList) {
                searchBuild.addSourceIncludePattern(para);
            }
        }
        SearchResult searchResult = searchWithScroll(searchBuild);
        return getRecordsFromScroll(searchResult);
    }

    private List<Map<String, Object>> getRecordsFromScroll(SearchResult searchResult) {
        List<Map<String, Object>> res = new ArrayList<Map<String, Object>>();
        try {
            if (!searchResult.isSucceeded()) {
                logger.error("Query Fail In Es: " + searchResult.getErrorMessage());
                return res;
            }
            JsonElement scrollIdJson = searchResult.getJsonObject().get("_scroll_id");
            if (scrollIdJson == null) {
                return res;
            }
            String scrollId = scrollIdJson.getAsString();
            int hitsNumber;
            List<Map> hit = searchResult.getSourceAsObjectList(Map.class, true);
            do {
                for (Map a : hit) {
                    try {
                        res.add(a);
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                        e.printStackTrace();
                        logger.error("Error When Parse Es Search Hit To Object: " + e.getMessage(), e);
                    }
                }
                SearchScroll scroll = new SearchScroll.Builder(scrollId, "5m").build();
                JestResult scrollResult = client.execute(scroll);
                hit = scrollResult.getSourceAsObjectList(Map.class, true);
                hitsNumber = hit == null ? 0 : hit.size();
            }
            while (hitsNumber != 0);
        } catch (Exception e) {
            logger.warn("Get Query Scroll Id Error." + e.getMessage(), e);
        }
        return res;
    }

    private SearchResult searchWithScroll(Search.Builder builder) throws IOException {
        builder.setParameter(Parameters.SCROLL, "5m");
        Search search = builder.build();
        return client.execute(search);
    }

}
