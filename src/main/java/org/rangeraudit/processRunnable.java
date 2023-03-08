package org.rangeraudit;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class processRunnable implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(processRunnable.class);

    private final String jaasConfPath;
    private final String solrPath;
    private final String logPath;
    private final String localDir;
    private final CloudClient cloudClient;

    public processRunnable(String logPath, String localDir, CloudClient cloudClient, String jaasConfPath, String solrPath) {
        this.jaasConfPath = jaasConfPath;
        this.solrPath = solrPath;
        this.logPath = logPath;
        this.localDir = localDir;
        this.cloudClient = cloudClient;
    }

    @Override
    public void run() {
        try {
            this.cloudClient.downloadFromCloud(logPath, localDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
//        updateSolr(logPath, jaasConfPath, solrPath);
    }
}
