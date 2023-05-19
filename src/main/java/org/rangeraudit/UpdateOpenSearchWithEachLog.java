package org.rangeraudit;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class UpdateOpenSearchWithEachLog {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateOpenSearchWithEachLog.class);

    public static void updateOpenSearch(String localLogPath) {
        /**
         * Insert each log file into OpenSearch.
         *
         * @param logPathStr Path of the log file. For example "tmp_logs/20230111/hbaseRegional_ranger_audit_XYZ.log".
         * @throws IOException If an I/O error occurs.
         */
        try {
            OpenSearchClient client = getClient();
            JSONParser jsonParser = new JSONParser();
            BufferedReader reader = new BufferedReader(new FileReader(localLogPath));
            String line = reader.readLine();

            final String INDEX = "ranger_audits";
            Integer documentsPerBulk = 10000;

            while (line != null) {
                ArrayList<BulkOperation> bulkOperationsList = new ArrayList<>();
                Integer counter = 0;
                while (line != null && counter < documentsPerBulk) {
                    JSONObject document;
                    try {
                        document = (JSONObject) jsonParser.parse(line);
                        IndexOperation<Map<String, Object>> indexOperation = new IndexOperation.Builder<Map<String, Object>>()
                                .index(INDEX).id((String) document.get("id")).document(document).build();
                        BulkOperation bulkOperation = new BulkOperation.Builder().index(indexOperation).build();
                        bulkOperationsList.add(bulkOperation);
                    } catch (ParseException e) {
                        LOG.error("Parsing Json failed.");
                        throw new RuntimeException(e);
                    }
                    counter += 1;
                    line = reader.readLine();
                }
                BulkRequest bulkRequest = new BulkRequest.Builder().operations(bulkOperationsList).build();
                client.bulk(bulkRequest);
            }
            reader.close();
            LOG.info("Inserted " + localLogPath + " into Open Search.");
        } catch (IOException e) {
            LOG.error("Failed at inserting Open Search: " + localLogPath);
            throw new RuntimeException(e);
        }
    }

    private static OpenSearchClient getClient() {
        final HttpHost host = new HttpHost("search-sdxsaasgracetest1-qfx5zw4df23lgtdgp7aulkt2yu.us-west-2.es.amazonaws.com",
                443, "https");
        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(new AuthScope(host), new UsernamePasswordCredentials("", ""));
        final RestClient restClient = RestClient.builder(host).
                setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                }).build();

        final OpenSearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        final OpenSearchClient client = new OpenSearchClient(transport);
        return client;
    }
}
