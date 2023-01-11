package org.example;


import okhttp3.Response;
import org.json.simple.parser.ParseException;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.security.PrivilegedActionException;

import static org.example.UpdateSolr.callWithKerberos;
import static org.example.Utilities.*;

public class Main {

    final String localDir = "tmp_logs";
    public static void main(String[] args) throws IOException, InterruptedException, ParseException, PrivilegedActionException, LoginException {
//        String data = readFileAsJsonList("test.log");

        String data = "[{\"repoType\":1,\"repo\":\"cm_hdfs\",\"reqUser\":\"hbase\",\"evtTime\":\"2022-12-07 16:37:32.806\",\"access\":\"delete\",\"resource\":\"/hbase/.tmp\",\"resType\":\"path\",\"action\":\"write\",\"result\":1,\"agent\":\"hdfs\",\"policy\":-1,\"reason\":\"/hbase/.tmp\",\"enforcer\":\"hadoop-acl\",\"cliIP\":\"10.117.226.79\",\"reqData\":\"delete/CLI\",\"agentHost\":\"gracezhugracezhutesttest\",\"logType\":\"RangerAudit\",\"id\":\"dc35b082-8599-40c2-8726-0720f7bf640b-0\",\"seq_num\":1,\"event_count\":1,\"event_dur_ms\":1,\"tags\":[],\"additional_info\":\"{\\\"forwarded-ip-addresses\\\":\\\"[]\\\",\\\"remote-ip-address\\\":\\\"10.117.226.79\\\",\\\"accessTypes\\\":\\\"[read, write, execute]\\\"}\",\"cluster_name\":\"gracezhu-aws-env6\"}]";
        callWithKerberos(data);
//
//        AzureClient azureClient = new AzureClient("data@gracezhuresourcegroupsan.dfs.core.windows.net",
//                "");
//
//        azureClient.downloadFromCloud("tmp_logs", 2);


    }

}
