package org.rangeraudit;

import static org.rangeraudit.UpdateOpenSearchWithEachLog.updateOpenSearch;
import static org.rangeraudit.Utilities.deleteLogFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessRunnable implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessRunnable.class);
    private final String cloudLogPath;
    private final String localDir;
    private final CloudClient cloudClient;

    public ProcessRunnable(String cloudLogPath, String localDir, CloudClient cloudClient) {
        this.cloudLogPath = cloudLogPath;
        this.localDir = localDir;
        this.cloudClient = cloudClient;
    }

    @Override
    public void run() {
        try {
            File localLogPath = cloudClient.downloadFromCloud(cloudLogPath, localDir);
            if (localLogPath != null && Files.exists(Paths.get(localLogPath.toString()))) {
                updateOpenSearch(localLogPath.toString());
                deleteLogFile(localLogPath);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
