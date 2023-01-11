package org.rangeraudit;

import net.sourceforge.argparse4j.inf.Namespace;
import org.json.simple.parser.ParseException;

import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedActionException;

import static java.lang.System.exit;
import static org.rangeraudit.Utilities.*;

public class Main {

    static final String localDir = "tmp_logs";
    public static void main(String[] args) throws IOException, InterruptedException, ParseException, PrivilegedActionException, LoginException {

        // Get the jaas.conf file path
        final String jaasConfPath = getJaasConf();
        if (jaasConfPath == "") {
            System.out.println("Failed to find Solr jaas.conf. Program exits.");
//            exit(0);
        }

        System.out.println("Using " + jaasConfPath + " for Kerberos authentication.");

        // Get user inputs.
        Namespace inputs = getUserInputs(args);
        String cloudType = inputs.get("cloud_type");
        String storageLocation = inputs.get("storage_location");
        String accessKeyId = inputs.get("access_key_id");
        Integer daysAgo = inputs.get("days_ago");
        String solrPath = inputs.get("solr_path");

        // Download logs.
        if (cloudType.equalsIgnoreCase("aws")) {
            String secretAccessKey = inputs.get("secret_access_key");
            AWSClient awsClient = new AWSClient(storageLocation, accessKeyId, secretAccessKey);
            awsClient.downloadFromCloud(daysAgo,localDir);
        } else {
            AzureClient azureClient = new AzureClient(storageLocation, accessKeyId);
            azureClient.downloadFromCloud(daysAgo,localDir);
        }

        // Update Solr using SolrJ.




//        deleteDirectory(new File(localDir));


    }

}
