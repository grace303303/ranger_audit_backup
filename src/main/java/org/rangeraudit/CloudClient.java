package org.rangeraudit;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public interface CloudClient {
    ArrayList<String> getAllValidLogPaths(int daysAgo);

    File downloadFromCloud(String s3logPath, String localDir) throws IOException;
}

