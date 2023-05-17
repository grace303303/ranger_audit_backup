package org.rangeraudit;

import org.apache.http.HttpHost;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.CreateRequest;
import org.opensearch.client.opensearch.core.CreateResponse;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.IndexResponse;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.IndexSettings;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.Transport;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.opensearch.client.util.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

public class UpdateOpenSearchWithEachLog {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateOpenSearchWithEachLog.class);

    public static void updateOpenSearch(String localLogPath) {
        /**
         * Insert each log file into OpenSearch.
         *
         * @param logPathStr Path of the log file. For example "tmp_logs/20230111/hbaseRegional_ranger_audit_XYZ.log".
         * @throws IOException If an I/O error occurs.
         */
        try{
            OpenSearchClient client = getClient();
            final String INDEX = "ranger_audits";

            JSONParser jsonParser = new JSONParser();
            BufferedReader reader = new BufferedReader(new FileReader(localLogPath));
            String line = reader.readLine();

            while (line != null) {
                JSONObject document;
                try {
                    document = (JSONObject) jsonParser.parse(line);
                } catch (ParseException e) {
                    LOG.error("Parsing Json failed.");
                    throw new RuntimeException(e);
                }

////                BulkRequest.Builder bulkRequestBuilder = new BulkRequest.Builder();
//
//                BulkRequest bulkRequest = new BulkRequest.Builder();



                IndexRequest<JSONObject> indexRequest = new IndexRequest.Builder<JSONObject>().index(INDEX).document(document).build();
                client.index(indexRequest);
                line = reader.readLine();
            }
            reader.close();
            LOG.info("Inserted " + localLogPath + " into Open Search.");
        } catch (IOException e){
            LOG.error("Failed at inserting Open Search: " + localLogPath);
            throw new RuntimeException(e);
        }
    }

    private static OpenSearchClient getClient() {
        /**
         * Get the RestClient.
         *
         */
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200, "http")).
                setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(new BasicCredentialsProvider());
                    }
                }).build();
        OpenSearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        OpenSearchClient client = new OpenSearchClient(transport);

        return client;
    }
}
