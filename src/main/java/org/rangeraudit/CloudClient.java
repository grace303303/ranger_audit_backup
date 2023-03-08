package org.rangeraudit;

import java.io.IOException;
import java.util.ArrayList;

public interface CloudClient {
    ArrayList<String> getAllValidLogPaths(int daysAgo);

    void downloadFromCloud(String s3logPath, String localDir) throws IOException;
}

