package org.rangeraudit;

import net.sourceforge.argparse4j.inf.Namespace;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.rangeraudit.UpdateSolr.updateSolr;
import static org.rangeraudit.Utilities.*;

public class Main {

    static final String localDir = "tmp_logs";

    public static void main(String[] args) throws IOException {
        try {

            // Get the jaas.conf file path
            final String jaasConfPath = getJaasConf();
            if (jaasConfPath == "") {
                System.out.println("Failed to find Solr jaas.conf. Program exits.");
//                exit(0);
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
                awsClient.downloadFromCloud(daysAgo, localDir);
            } else {
                AzureClient azureClient = new AzureClient(storageLocation, accessKeyId);
                azureClient.downloadFromCloud(daysAgo, localDir);
            }

            // Update Solr using SolrJ.
            Stream<Path> filepath = Files.walk(Paths.get(localDir));

            filepath.forEach(path -> {
                File file = new File(path.toUri());
                if (file.isFile() && file.getName().contains(".log")) {
                    updateSolr(path.toString(), jaasConfPath, solrPath);
                }
            });

        } finally {
            // Delete "/tmp_logs".
            deleteDirectory(new File(localDir));

        }

    }

}
