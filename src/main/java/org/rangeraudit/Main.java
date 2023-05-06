// TODO package name must identify the project. You are not realy working in the "organization RangerAudit". Name should be appropriate.
//  ie. name.grace3030.rangerauditimport shows this is your own personal progect about rangerauditimport
// com.cludera.trhunderhead.rangerauditimport shows this is for a "Cloudera company" project thunderhead and subproject rangeraudirimport or something
package org.rangeraudit;

// TODO static imports are mainly for constants, not methods - remov these and use System.exit, Utilities.getJaasConf etc
import static java.lang.System.exit;
import static org.rangeraudit.Utilities.deleteDirectory;
import static org.rangeraudit.Utilities.getJaasConf;
import static org.rangeraudit.Utilities.getUserInputs;

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
    // TODO why do you have both local variable and class field? Only use one of them and remove the other
    // class constants must be uppercase_camelcase


    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        String LOCAL_DIR = "tmp_logs";
        try {
            // Get the jaas.conf file path
            final String jaasConfPath = getJaasConf();  // TODO this file must be possible to select via command line arguments
            if (jaasConfPath.equals("")) {  // TODO better use jaasConfPath.isBlank()
                LOG.info("Failed to find Solr jaas.conf. Program exits.");
                exit(1);
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

            if (totalThreads == null) { // TODO remove this condition  and use default value (see Utilities.getUserInputs)
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
            while (!executorService.isTerminated()) {   // TODO do not use while loop with empty body - it will burn your CPU. do Thread.sleep(1000) inside to make it rest while waiting
            }
            LOG.info("Program completed!");
        } finally {
            // Delete "/tmp_logs".
            deleteDirectory(new File(localDir));
        }
    }
}
