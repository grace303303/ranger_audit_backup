package org.rangeraudit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.rangeraudit.UpdateSolrWithEachLog.updateSolr;

public class LogRunnable implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LogRunnable.class);

    private final String jaasConfPath;
    private final String solrPath;
    private final String logPath;

    public LogRunnable(String jaasConfPath, String solrPath, String logPath) {
        this.jaasConfPath = jaasConfPath;
        this.solrPath = solrPath;
        this.logPath = logPath;
    }

    @Override
    public void run() {
        updateSolr(logPath, jaasConfPath, solrPath);
    }
}
