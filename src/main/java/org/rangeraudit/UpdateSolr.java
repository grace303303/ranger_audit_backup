package org.rangeraudit;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

public class UpdateSolr {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateSolr.class);

    public static void updateSolr(String logPathStr, String jaasConfPath, String solrPath) {
        /**
         * Insert each log file into Solr.
         *
         * @param logPathStr Path of the log file. For example "tmp_logs/20230111/hbaseRegional_ranger_audit_XYZ.log".
         * @param jaasConfPath The jaas.conf path, which will be used for Kerberos authentication.
         * @param solrPath Solr URL path, a combination of the hostname and port number, for example "master0.XYZ.dev.cldr.work:8985".
         * @throws IOException If an I/O error occurs.
         */
        SolrClient solrClient = getSolrClient(jaasConfPath, solrPath);
        Path filePath = Paths.get(logPathStr);
        JSONParser jsonParser = new JSONParser();
        Collection<SolrInputDocument> docs = new ArrayList<>();

        try {
            Stream<String> stream = Files.lines(filePath, StandardCharsets.UTF_8);
            stream.forEach(line -> {
                SolrInputDocument document = new SolrInputDocument();

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
                    document.addField(key, value);
                }
                docs.add(document);

            });
        } catch (IOException e) {
            LOG.error("Reading the log failed.");
            throw new RuntimeException(e);
        }

        try {
            solrClient.add(docs);
            LOG.info("Inserted " + logPathStr + " into Solr.");
        } catch (SolrServerException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private static SolrClient getSolrClient(String jaasConfPath, String solrPath) {
        /**
         * Get the SolrClient with Kerberos authentication..
         *
         * @param jaasConfPath The jaas.conf path, which will be used for Kerberos authentication.
         * @param solrPath Solr URL path, a combination of the hostname and port number, for example "master0.XYZ.dev.cldr.work:8985".
         */

        System.setProperty("java.security.auth.login.config", jaasConfPath);
        String urlString = "https://" + solrPath + "/solr/ranger_audits";

        HttpSolrClient.Builder solrClientBuilder = new HttpSolrClient.Builder(urlString);
        Krb5HttpClientBuilder krbBuilder = new Krb5HttpClientBuilder();
        SolrHttpClientBuilder krb5HttpClientBuilder = krbBuilder.getHttpClientBuilder(java.util.Optional.empty());
        HttpClientUtil.setHttpClientBuilder(krb5HttpClientBuilder);
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(HttpClientUtil.PROP_FOLLOW_REDIRECTS, false);
        CloseableHttpClient httpClient = HttpClientUtil.createClient(params);
        SolrClient client = solrClientBuilder.withHttpClient(httpClient).build();

        return client;

    }


}
