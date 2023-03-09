package org.rangeraudit;

import static org.rangeraudit.UpdateSolrWithEachLog.updateSolr;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessRunnable implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessRunnable.class);

    private final String jaasConfPath;
    private final String solrPath;
    private final String cloudLogPath;
    private final String localDir;
    private final CloudClient cloudClient;

    public ProcessRunnable(String cloudLogPath, String localDir, CloudClient cloudClient, String jaasConfPath, String solrPath) {
        this.jaasConfPath = jaasConfPath;
        this.solrPath = solrPath;
        this.cloudLogPath = cloudLogPath;
        this.localDir = localDir;
        this.cloudClient = cloudClient;
    }

    @Override
    public void run() {
        try {
            String localLogPath = cloudClient.downloadFromCloud(cloudLogPath, localDir);
            if (localLogPath != null && Files.exists(Paths.get(localLogPath))) {
                updateSolr(localLogPath, jaasConfPath, solrPath);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
