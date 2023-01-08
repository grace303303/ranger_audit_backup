package org.example;

import com.squareup.okhttp.*;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.security.UserGroupInformation;


import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
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



    }
}
