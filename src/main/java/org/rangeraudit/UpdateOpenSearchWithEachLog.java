package org.rangeraudit;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class UpdateOpenSearchWithEachLog {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateOpenSearchWithEachLog.class);

    public static void updateOpenSearch(String localLogPath) {
        /**
         * Insert each log file into OpenSearch.
         *
         * @param logPathStr Path of the log file. For example "tmp_logs/20230111/hbaseRegional_ranger_audit_XYZ.log".
         * @throws IOException If an I/O error occurs.
         */
        JSONParser jsonParser = new JSONParser();

        try {
            // Read the log file line by line to avoid the file being too large issue.
            BufferedReader reader = new BufferedReader(new FileReader(localLogPath));
            String line = reader.readLine();
            RestHighLevelClient client = getClient();

            while (line != null) {
                HashMap<String, Object> document = new HashMap();
                JSONObject jsonObject;
                try {
                    jsonObject = (JSONObject) jsonParser.parse(line);
                } catch (ParseException e) {
                    LOG.error("Parsing Json failed.");
                    throw new RuntimeException(e);
                }
                for (Iterator iterator = jsonObject.keySet().iterator(); iterator.hasNext(); ) {
                    String key = (String) iterator.next();
                    Object value = jsonObject.get(key);
                    document.put(key, value);
                }
                IndexRequest request = new IndexRequest("ranger_audits");
                request.source(document); //Place your content into the index's source.
                IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT); //Add the document to the index.
                LOG.trace("Added document into index: {}", indexResponse);

                line = reader.readLine();

            }
            client.close();
            reader.close();
            LOG.info("Inserted " + localLogPath + " into Open Search.");

        } catch (IOException e) {
            LOG.error("Failed at inserting Open Search: " + localLogPath);
            throw new RuntimeException(e);
        }

    }

    private static RestHighLevelClient getClient() {
        /**
         * Get the RestHighLevelClient.
         *
         */
        RestClientBuilder builder = RestClient.builder(new HttpHost("host.docker.internal", 9200, "http"))
                .setRequestConfigCallback(
                        new RestClientBuilder.RequestConfigCallback() {
                            @Override
                            public RequestConfig.Builder customizeRequestConfig(
                                    RequestConfig.Builder requestConfigBuilder) {
                                return requestConfigBuilder
                                        .setConnectTimeout(5000)
                                        .setSocketTimeout(600000);
                            }
                        });

        RestHighLevelClient client = new RestHighLevelClient(builder);

        return client;
    }
}
