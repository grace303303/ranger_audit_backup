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
    public static void updateSolr(String logPathStr, String jaasConfPath, String solrPath)  {
        SolrClient solrClient = getSolrClient(jaasConfPath, solrPath);
        Path filePath = Paths.get(logPathStr);
        JSONParser jsonParser = new JSONParser();
        Collection<SolrInputDocument> docs = new ArrayList<>();

        try (Stream<String> stream = Files.lines(filePath, StandardCharsets.UTF_8)) {
            stream.forEach(line -> {
                SolrInputDocument document = new SolrInputDocument();
                try {
                    JSONObject jsonObject = (JSONObject)jsonParser.parse(line);

                    for(Iterator iterator = jsonObject.keySet().iterator(); iterator.hasNext();) {
                        String key = (String) iterator.next();
                        Object value = jsonObject.get(key);
                        document.addField(key, value);
                    }
                    docs.add(document);

                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            System.out.println("Reading the log fails.");
            throw new RuntimeException(e);
        }

        try {
            solrClient.add(docs);
            solrClient.commit();
            System.out.println("Inserted " + logPathStr + " into Solr.");
        } catch (SolrServerException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private static SolrClient getSolrClient(String jaasConfPath, String solrPath) {

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
