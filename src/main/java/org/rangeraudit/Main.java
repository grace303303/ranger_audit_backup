package org.rangeraudit;

import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static java.lang.System.exit;
import static org.rangeraudit.Utilities.*;


public class Main {

    static final String localDir = "tmp_logs";

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        String LOCAL_DIR = "tmp_logs";
        try {

            // Get the jaas.conf file path
            final String jaasConfPath = getJaasConf();
            if (jaasConfPath.equals("")) {
                LOG.info("Failed to find Solr jaas.conf. Program exits.");
//                exit(1);
            }

            LOG.info("Using " + jaasConfPath + " for Kerberos authentication.");

            // Get user inputs.
            final Namespace inputs = getUserInputs(args);
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
                    LOG.error("Please provide AWS secret access key.");
                    exit(1);
                }
                AWSClient awsClient = new AWSClient(storageLocation, accessKeyId, secretAccessKey);
                ArrayList allValidLogPaths = awsClient.getAllValidLogPaths(daysAgo);

                LOG.info("Start the download, upload, and deletion process using " + totalThreads + " threads.");
                allValidLogPaths.forEach(validLogPath -> {
                            Runnable logRunnable = new processRunnable(validLogPath.toString(), LOCAL_DIR, awsClient, jaasConfPath, solrPath);
                            executorService.execute(logRunnable);
                        }
                );
            } else {
                AzureClient azureClient = new AzureClient(storageLocation, accessKeyId);
                ArrayList allValidLogPaths = azureClient.getAllValidLogPaths(daysAgo);

                LOG.info("Start the download, upload, and deletion process using " + totalThreads + " threads.");
                allValidLogPaths.forEach(validLogPath -> {
                    Runnable logRunnable = new processRunnable(validLogPath.toString(), LOCAL_DIR, azureClient, jaasConfPath, solrPath);
                    executorService.execute(logRunnable);
                });
            }
            executorService.shutdown();
            while (!executorService.isTerminated()) {
            }
            LOG.info("Program completed!");
//
//            // Update Solr using SolrJ.
//            Path localPath = Paths.get(localDir);
//            if (!Files.exists(localPath)) {
//                LOG.error("Failed to find " + localDir + ". Program exits.");
//                exit(1);
//            }
//
//            if (totalThreads == null) {
//            // Thread number is defaulted to be 1.
//                totalThreads = 1;
//            }
//
//            LOG.info("Start insertion into Solr using " + totalThreads + " threads.");
//            ExecutorService executorService = Executors.newFixedThreadPool(totalThreads);
//
//            Stream<Path> filePaths = Files.walk(localPath);
//            filePaths.forEach(path -> {
//                File file = new File(path.toUri());
//                if (file.isFile() && file.getName().contains(".log")) {
//                    Runnable logRunnable = new LogRunnable(jaasConfPath, solrPath, path.toString());
//                    executorService.execute(logRunnable);
//                }
//            });
//

        } finally {
            // Delete "/tmp_logs".
            deleteDirectory(new File(localDir));
        }
    }
}
