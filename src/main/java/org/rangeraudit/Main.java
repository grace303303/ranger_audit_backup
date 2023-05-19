package org.rangeraudit;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sourceforge.argparse4j.inf.Namespace;


public class Main {

    static final String localDir = "tmp_logs";

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        String LOCAL_DIR = "tmp_logs";
        try {
            // Get the jaas.conf file path
            final String jaasConfPath = Utilities.getJaasConf();
            if (jaasConfPath.equals("")) {
                LOG.info("Failed to find Solr jaas.conf. Program exits.");
//                System.exit(1);
            }

            LOG.info("Using " + jaasConfPath + " for Kerberos authentication.");

            // Get user inputs.
            final Namespace inputs = Utilities.getUserInputs(args);
            final String cloudType = inputs.get("cloud_type");
            final String storageLocation = inputs.get("storage_location");
            final String accessKeyId = inputs.get("access_key_id");
            final Integer daysAgo = inputs.get("days_ago");
            final String solrPath = inputs.get("solr_path");
            Integer totalThreads = inputs.get("threads");

            if (totalThreads == null) {
                // Thread number is defaulted to be 1.
                totalThreads = 1;
            }

            // Start the process multithreading.
            ExecutorService executorService = Executors.newFixedThreadPool(totalThreads);
            if (cloudType.equalsIgnoreCase("aws")) {
                String secretAccessKey = inputs.get("secret_access_key");
                if (secretAccessKey == null) {
                    LOG.error("Missing paramter --secretAccessKey.");
                    System.exit(1);
                }
                String region = inputs.get("region");
                if (region == null) {
                    LOG.error("Missing paramter --region.");
                    System.exit(1);
                }
                AWSClient awsClient = new AWSClient(storageLocation, accessKeyId, secretAccessKey, region);
                ArrayList allValidLogPaths = awsClient.getAllValidLogPaths(daysAgo);

                LOG.info("Start the AWS download, upload, and deletion process using " + totalThreads + " threads.");
                allValidLogPaths.forEach(validLogPath -> {
                            Runnable logRunnable = new ProcessRunnable(validLogPath.toString(), LOCAL_DIR, awsClient, jaasConfPath, solrPath);
                            executorService.execute(logRunnable);
                        }
                );
            } else {
                AzureClient azureClient = new AzureClient(storageLocation, accessKeyId);
                ArrayList allValidLogPaths = azureClient.getAllValidLogPaths(daysAgo);

                LOG.info("Start the AZURE download, upload, and deletion process using " + totalThreads + " threads.");
                allValidLogPaths.forEach(validLogPath -> {
                    Runnable logRunnable = new ProcessRunnable(validLogPath.toString(), LOCAL_DIR, azureClient, jaasConfPath, solrPath);
                    executorService.execute(logRunnable);
                });
            }
            executorService.shutdown();
            while (!executorService.isTerminated()) {
            }
            LOG.info("Program completed!");
        } finally {
            // Delete "/tmp_logs".
            Utilities.deleteDirectory(new File(localDir));
        }
    }
}
