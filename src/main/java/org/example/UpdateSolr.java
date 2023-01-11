package org.example;

import com.sun.security.auth.callback.TextCallbackHandler;
import okhttp3.*;
import org.apache.hadoop.security.UserGroupInformation;

import javax.net.ssl.*;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.cert.CertificateException;

import static javax.security.auth.Subject.doAs;


public class UpdateSolr {
    public static void callWithKerberos(String data) throws IOException, LoginException, PrivilegedActionException, InterruptedException {

        System.setProperty("java.security.auth.login.config", "../../run/cloudera-scm-agent/process/1546335586-solr-SOLR_SERVER/jaas.conf");
        LoginContext lc = new LoginContext("Client", new TextCallbackHandler());
        lc.login();
        Subject subject = lc.getSubject();
        System.out.println("Subject: " + subject);

        Object returnObject = Subject.doAs(subject, new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                callAPI(data);
                return null;
            }
        });

//        UserGroupInformation.loginUserFromKeytab("solr/gracezhu-aws-env-longrunning-master0.gracezhu.xcu2-8y8x.dev.cldr.work@GRACEZHU.XCU2-8Y8X.DEV.CLDR.WORK", "/var/run/cloudera-scm-agent/process/1546335586-solr-SOLR_SERVER/solr.keytab");
//        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
//
//        ugi.doAs(new PrivilegedExceptionAction<Void>() {
//            public Void run() throws Exception {
//                callAPI(data);
//                return null;
//            }
//        });

    }

    private static Response callAPI(String data) throws IOException {

        OkHttpClient client = getUnsafeOkHttpClient();
        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create(data, mediaType);
        Request request = new Request.Builder()
                .url("https://gracezhu-aws-env-longrunning-master0.gracezhu.xcu2-8y8x.dev.cldr.work:8985/solr/ranger_audits/update?commitWithin=1000&overwrite=true&wt=json")
                .post(body)
                .build();
        Response response = client.newCall(request).execute();

        System.out.println(response);

        return response;

    }

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

}
