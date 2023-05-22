// Copyright (c) 2023 Cloudera, Inc. All rights reserved.
package org.rangeraudit;

import static org.rangeraudit.Utilities.isDateStr;
import static org.rangeraudit.Utilities.isLaterDate;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;

public class AzureClient implements CloudClient {
    /**
     * The Azure Client used to run commands for Azure.
     */
    private static final Logger LOG = LoggerFactory.getLogger(
            AzureClient.class);
    private final String containerName;
    private final String accountName;
    private final StorageSharedKeyCredential credentials;

    public AzureClient(final String storageLocation, final String accessKeyID) {
        this.containerName = getContainerName(storageLocation);
        this.accountName = getAccountName(storageLocation);
        this.credentials = new StorageSharedKeyCredential(
                storageLocation, accessKeyID);
    }

    /**
     * Download logs from Azure Blob.
     *
     * @param daysAgo How many days ago we want to start downloading the logs,
     *                for exmaple, put "0" will download
     * today's logs, and put "2" will download the logs of today, yesterday,
     *                and the day before yesterday's.
     * @return An ArrayList of all the valid blob log path.
     */
    @Override
    public ArrayList<String> getAllValidLogPaths(final int daysAgo) {
        ArrayList<String> allValidLogPaths = new ArrayList();
        String endpoint = String.format(Locale.ROOT,
                "https://%s.blob.core.windows.net", accountName);

        BlobServiceClient storageClient = new BlobServiceClientBuilder()
                .endpoint(endpoint).credential(credentials).buildClient();
        BlobContainerClient blobContainerClient = storageClient
                .getBlobContainerClient(containerName);

        ListBlobsOptions options = new ListBlobsOptions()
                .setPrefix("ranger/audit/");

        final PagedIterable<BlobItem> blobs = blobContainerClient
                .listBlobs(options, null);

        for (BlobItem blob : blobs) {
            String[] blobPathList = blob.getName().split("/");
            String potentialDateStr = blobPathList[blobPathList.length - 2];
            String fileName = blobPathList[blobPathList.length - 1];

            if (!isDateStr(potentialDateStr) || !isLaterDate(
                    potentialDateStr, daysAgo)) {
                continue;
            }

            if (Objects.equals(fileName, "")) {
                continue;
            }

            allValidLogPaths.add(blob.getName());
        }
        return allValidLogPaths;
    }

    /**
     * Download logs from Azure Blob.
     *
     * @param blobLogPath The log path on Azure blob, for example.
     * @param localDir The local location to store the downloaded files
     *                 temporarily, this is defaulted to "tmp_logs".
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public File downloadFromCloud(final String blobLogPath,
            final String localDir) throws IOException {
        File localFilePath;

        String endpoint = String.format(Locale.ROOT,
                "https://%s.blob.core.windows.net", accountName);

        BlobServiceClient storageClient = new BlobServiceClientBuilder()
                .endpoint(endpoint).credential(credentials).buildClient();
        BlobContainerClient blobContainerClient = storageClient
                .getBlobContainerClient(containerName);

        BlockBlobClient blobClient = blobContainerClient
                .getBlobClient(blobLogPath).getBlockBlobClient();
        String[] blobPathList = blobLogPath.split("/");
        String potentialDateStr = blobPathList[blobPathList.length - 2];
        String fileName = blobPathList[blobPathList.length - 1];

        // Create /tmp_logs/yyyymmdd directory to store logs.
        File tmpFolder = new File(localDir);
        tmpFolder.mkdir();
        File dateFolder = new File(tmpFolder + "/" + potentialDateStr);
        dateFolder.mkdir();
        localFilePath = new File(dateFolder + "/" + fileName);
        localFilePath.createNewFile();
        try {
            blobClient.downloadToFile(localFilePath.toString(), true);
            LOG.info("Downloaded log: " + localFilePath);
        } catch (Exception e) {
            LOG.info("Failed at downloading: " + blobLogPath);
            e.printStackTrace();
        }
        return localFilePath;
    }

    private String getContainerName(final String storageLocation) {
        String[] blobLocationList = storageLocation.split("@", 2);
        return blobLocationList[0];
    }

    private String getAccountName(final String storageLocation) {
        String[] blobLocationList = storageLocation.split("@", 2);
        return blobLocationList[1].split("\\.")[0];
    }
}
