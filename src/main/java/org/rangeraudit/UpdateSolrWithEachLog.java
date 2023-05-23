// Copyright (c) 2023 Cloudera, Inc. All rights reserved.
package org.rangeraudit;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public final class UpdateSolrWithEachLog {
    /**
     * Insert data into Solr with Solr Client.
     */
    private static final Logger LOG = LoggerFactory.getLogger(
            UpdateSolrWithEachLog.class);
    /**
     * Insert each log file into Solr.
     *
     * @param localLogPath Path of the log file, for example
     *                     "tmp_logs/20230111/XYZ.log".
     * @param jaasConfPath The jaas.conf path, which will be used for Kerberos
     *                     authentication.
     * @param solrPath Solr URL path, a combination of the hostname and
     *                 port, for example "master0.XYZ.dev.cldr.work:8985".
     * @param documentsPerBatch Number of documents for each batch of insertion,
     *                          this is defaulted to be 1000.
     * @throws IOException If an I/O error occurs.
     */
    public static void updateSolr(String localLogPath, String jaasConfPath,
            String solrPath, Integer documentsPerBatch) {
        ConcurrentUpdateSolrClient solrClient = getConcurrentSolrClient(jaasConfPath, solrPath);
        JSONParser jsonParser = new JSONParser();

        try {
            // Read the log file line by line to avoid the file being too large.
            BufferedReader reader = new BufferedReader(
                    new FileReader(localLogPath));
            String line = reader.readLine();
            while (line != null) {
                ArrayList<SolrInputDocument> batch = new ArrayList<>();
                Integer counter = 0;

                while (line != null && counter < documentsPerBatch) {
                    SolrInputDocument document = new SolrInputDocument();
                    JSONObject jsonObject;
                    try {
                        jsonObject = (JSONObject) jsonParser.parse(line);
                    } catch (ParseException e) {
                        LOG.error("Parsing Json failed.");
                        throw new RuntimeException(e);
                    }

                    for (Iterator iterator = jsonObject.keySet().iterator();
                         iterator.hasNext();) {
                        String key = (String) iterator.next();
                        Object value = jsonObject.get(key);
                        document.addField(key, value);
                    }
                    // Add the document from a json object into the batch.
                    batch.add(document);
                    counter += 1;
                    line = reader.readLine();
                }
                //Add the batch (a list with maximum documentsPerBatch
                // of documents) into the client.
                solrClient.add(batch);
            }
            solrClient.commit();
            LOG.info("Inserted " + localLogPath + " into Solr.");
            reader.close();
        } catch (IOException | SolrServerException e) {
            LOG.error("Failed at inserting log: " + localLogPath);
            throw new RuntimeException(e);
        }
    }
    /**
     * Get the SolrClient with Kerberos authentication..
     *
     * @param jaasConfPath The jaas.conf path, which will be used for
     *                     Kerberos authentication.
     * @param solrPath Solr URL path, a combination of the hostname and
     *                 port, for example "master0.XYZ.dev.cldr.work:8985".
     * @return The Solr client that has a ConcurrentUpdateSolrClient type.
     */
    private static ConcurrentUpdateSolrClient getConcurrentSolrClient(
            String jaasConfPath, String solrPath) {
        final Integer solrClientThreadCount = 3;
        System.setProperty("java.security.auth.login.config", jaasConfPath);
        String urlString = "https://" + solrPath + "/solr/ranger_audits";

        ConcurrentUpdateSolrClient.Builder concurrentUpdateSolrClientBuilder =
                new ConcurrentUpdateSolrClient.Builder(urlString)
                        .withThreadCount(solrClientThreadCount);
        Krb5HttpClientBuilder krbBuilder = new Krb5HttpClientBuilder();
        SolrHttpClientBuilder krb5HttpClientBuilder = krbBuilder
                .getHttpClientBuilder(java.util.Optional.empty());
        HttpClientUtil.setHttpClientBuilder(krb5HttpClientBuilder);
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(HttpClientUtil.PROP_FOLLOW_REDIRECTS, false);
        CloseableHttpClient httpClient = HttpClientUtil.createClient(params);
        ConcurrentUpdateSolrClient client = concurrentUpdateSolrClientBuilder
                .withHttpClient(httpClient).build();

        return client;
    }
}
