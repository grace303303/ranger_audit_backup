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
        OkHttpClient client = getUnsafeOkHttpClient();
        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create(mediaType, "[{\"repoType\":8,\"repo\":\"cm_solr\",\"reqUser\":\"hue\",\"evtTime\":\"2022-12-06 17:30:04.238\",\"access\":\"query\",\"resource\":\"collections\",\"resType\":\"admin\",\"action\":\"query\",\"result\":0,\"agent\":\"solr\",\"policy\":-1,\"enforcer\":\"ranger-acl\",\"cliIP\":\"10.117.226.79\",\"agentHost\":\"gracezhu-aws-env6-master0\",\"logType\":\"RangerAudit\",\"id\":\"e53e9df9-7c59-4112-8fa8-3074f47cc0ff-0\",\"seq_num\":1,\"event_count\":1,\"event_dur_ms\":1,\"tags\":[],\"cluster_name\":\"gracezhu-aws-env6\"}]");
        Request request = new Request.Builder()
                .url("https://gracezhu-aws-env-gateway.gracezhu.xcu2-8y8x.dev.cldr.work/gracezhu-aws-env/cdp-proxy/solr/ranger_audits//update?commitWithin=1000&overwrite=true&wt=json")
                .method("POST", body)
                .addHeader("Cookie", "pac4jCsrfToken=c71a69e3-bf67-435c-a1cd-00eb030389cb; __utmc=23347436; __utma=23347436.2039860844.1673033335.1673033335.1673038150.2; __utmz=23347436.1673038150.2.2.utmcsr=console.dps.mow-dev.cloudera.com|utmccn=(referral)|utmcmd=referral|utmcct=/; hadoop-jwt=eyJraWQiOiJGdTd6cFBCX3N1U19lOFBPVE1SLW92UlAxM0FSSmN2czRQanlqQlFHb2JZIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiJjc3NvX2dyYWNlemh1Iiwia2lkIjoiRnU3enBQQl9zdVNfZThQT1RNUi1vdlJQMTNBUkpjdnM0UGp5akJRR29iWSIsImlzcyI6IktOT1hTU08iLCJleHAiOjE2NzMyODEyNzMsIm1hbmFnZWQudG9rZW4iOiJmYWxzZSIsImtub3guaWQiOiI3YzYzYTcyMS0zNmE4LTQ2YjMtODQ3MS1jZTk1NmYyZmM3NTMifQ.IDur6nenB5BaQ52o6c9vlijWKIg2PPHX0fqSmjB_8nIV65CsdFVHUswsvOgaoLrrc_dCsij7zNUzGiqrYqlI1fEd7r-sXFDNfZnubCo8OT9IEJUQh0mxSuoPu7pJIQKfFSyGcD-sqp2h6-gU5Km8EvEYRghaO5ajyED6I3CgHgcRAEWgY4NhNA2sgU0z8XhMP3RuQFTDP2_Azsrt4s0I2RLXIT0DgMvi8gS9xKQykWBQAkeb-CCCVCcJMKpH_VKTZrSeyB-Ih773f2jgMn306Z4PLEQfRMAfCUJ8KuTp0NsmFMJvwqwwjI9dqgGHz5oLmNv05V7-XGFThmjq14jzYQ; SESSION=ZTFhYjViZGYtNDQ5Ni00YWQwLThjN2MtNmNkNjllMWFkY2Qz; pac4jCsrfToken=84ec53a0-55cc-4f93-95d5-b0dcbaf7a982")
                .addHeader("Content-Type", "application/json")
                .build();
        Response response = client.newCall(request).execute();
        System.out.println(response);

    /*
        UserGroupInformation.loginUserFromKeytab("hdfs/gracezhu-aws-env-master0.gracezhu.xcu2-8y8x.dev.cldr.work@GRACEZHU.XCU2-8Y8X.DEV.CLDR.WORK", "/run/cloudera-scm-agent/process/1546335524-hdfs-DATANODE/hdfs.keytab");
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        ugi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {

                OkHttpClient client = new OkHttpClient();
                MediaType mediaType = MediaType.parse("application/json");
                RequestBody body = RequestBody.create(mediaType, "[{\"repoType\":8,\"repo\":\"cm_solr\",\"reqUser\":\"hue\",\"evtTime\":\"2022-12-06 17:30:04.238\",\"access\":\"query\",\"resource\":\"collections\",\"resType\":\"admin\",\"action\":\"query\",\"result\":0,\"agent\":\"solr\",\"policy\":-1,\"enforcer\":\"ranger-acl\",\"cliIP\":\"10.117.226.79\",\"agentHost\":\"gracezhu-aws-env6-master0\",\"logType\":\"RangerAudit\",\"id\":\"e53e9df9-7c59-4112-8fa8-3074f47cc0ff-0\",\"seq_num\":1,\"event_count\":1,\"event_dur_ms\":1,\"tags\":[],\"cluster_name\":\"gracezhu-aws-env6\"}]");
                Request request = new Request.Builder()
                        .url("https://gracezhu-aws-env-master0.gracezhu.xcu2-8y8x.dev.cldr.work:8985/solr/ranger_audits/update?commitWithin=1000&overwrite=true&wt=json")
                        .post(body)
                        .addHeader("Content-Type", "application/json")
                        .build();
                Response response = client.newCall(request).execute();

                System.out.println(response);

                return true;

            }
        });

    */
    }
}
