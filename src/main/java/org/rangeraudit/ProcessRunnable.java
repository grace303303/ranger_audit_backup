// Copyright (c) 2023 Cloudera, Inc. All rights reserved.
package org.rangeraudit;

import static org.rangeraudit.UpdateSolrWithEachLog.updateSolr;
import static org.rangeraudit.Utilities.deleteLogFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessRunnable implements Runnable {
    private final String jaasConfPath;
    private final String solrPath;
    private final String cloudLogPath;
    private final String localDir;
    private final CloudClient cloudClient;
    private final Integer documentsPerPatch;

    public ProcessRunnable(String cloudLogPath, String localDir, CloudClient cloudClient, String jaasConfPath, String solrPath,
            Integer documentsPerPatch) {
        this.jaasConfPath = jaasConfPath;
        this.solrPath = solrPath;
        this.cloudLogPath = cloudLogPath;
        this.localDir = localDir;
        this.cloudClient = cloudClient;
        this.documentsPerPatch = documentsPerPatch;
    }

    @Override
    public void run() {
        try {
            File localLogPath = cloudClient.downloadFromCloud(cloudLogPath, localDir);
            if (localLogPath != null && Files.exists(Paths.get(localLogPath.toString()))) {
                updateSolr(localLogPath.toString(), jaasConfPath, solrPath, documentsPerPatch);
                deleteLogFile(localLogPath);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
