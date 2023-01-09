package org.example;

import java.io.*;

import okhttp3.*;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import javax.net.ssl.*;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.security.cert.CertificateException;

public class UpdateSolr {
    private static OkHttpClient getUnsafeOkHttpClient() {
        try {
            // Create a trust manager that does not validate certificate chains
            final TrustManager[] trustAllCerts = new TrustManager[]{
                    new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) throws CertificateException {
                        }

                        @Override
                        public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) throws CertificateException {
                        }

                        @Override
                        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                            return new java.security.cert.X509Certificate[]{};
                        }
                    }
            };

            // Install the all-trusting trust manager
            final SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            // Create an ssl socket factory with our all-trusting manager
            final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

            OkHttpClient.Builder builder = new OkHttpClient.Builder();
            builder.sslSocketFactory(sslSocketFactory, (X509TrustManager) trustAllCerts[0]);
            builder.hostnameVerifier(new HostnameVerifier() {
                @Override
                public boolean verify(String hostname, SSLSession session) {
                    return true;
                }
            });

            OkHttpClient okHttpClient = builder.build();
            return okHttpClient;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        UserGroupInformation.loginUserFromKeytab("solr/gracezhu-aws-env-master0.gracezhu.xcu2-8y8x.dev.cldr.work@GRACEZHU.XCU2-8Y8X.DEV.CLDR.WORK", "/run/cloudera-scm-agent/process/1546336269-solr-SOLR_SERVER/solr.keytab");
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        ugi.doAs((PrivilegedExceptionAction<Object>) () -> {

            OkHttpClient client = getUnsafeOkHttpClient();
            MediaType mediaType = MediaType.parse("application/json");
            RequestBody body = RequestBody.create(mediaType, "[{\"repoType\":8,\"repo\":\"cm_solr\",\"reqUser\":\"gracegracetest\",\"evtTime\":\"2022-12-06 17:30:04.238\",\"access\":\"query\",\"resource\":\"collections\",\"resType\":\"admin\",\"action\":\"query\",\"result\":0,\"agent\":\"solr\",\"policy\":-1,\"enforcer\":\"ranger-acl\",\"cliIP\":\"10.117.226.79\",\"agentHost\":\"gracezhu-aws-env6-master0\",\"logType\":\"RangerAudit\",\"id\":\"e53e9df9-7c59-4112-8fa8-3074f47cc0ff-0\",\"seq_num\":1,\"event_count\":1,\"event_dur_ms\":1,\"tags\":[],\"cluster_name\":\"gracezhu-aws-env6\"}]");
            Request request = new Request.Builder()
                    .url("https://gracezhu-aws-env-gateway.gracezhu.xcu2-8y8x.dev.cldr.work/gracezhu-aws-env/cdp-proxy/solr/ranger_audits/update?commitWithin=1000&overwrite=true&wt=json")
                    .post(body)
                    .addHeader("Content-Type", "application/json")
                    .build();
            Response response = client.newCall(request).execute();

            System.out.println(response);

            return response;
        });

    }
}
