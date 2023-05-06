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
    private static final Logger LOG = LoggerFactory.getLogger(AzureClient.class);
    /**
     * The blob storage location where the data is stored, without the prefix.
     * (example: data@myresourcegroup.dfs.core.windows.net )
     */
    private final String storageLocation;
    /**
     * The Access Key of the storage account.
     */
    private final String accessKeyID;

    public AzureClient(String storageLocation, String accessKeyID) {
        this.storageLocation = storageLocation;
        this.accessKeyID = accessKeyID;
    }

    @Override
    public ArrayList<String> getAllValidLogPaths(int daysAgo) {
        /**
         * Download logs from AZURE Blob.
         *
         * @param daysAgo How many days ago we want to start downloading the logs, for exmaple, put "0" will download
         * today's logs, and put "2" will download the logs of today, yesterday, and the day before yesterday's.
         * @return An ArrayList of all the valid blob log path.
         */

        ArrayList<String> allValidLogPaths = new ArrayList();
        String[] blobLocationList = this.storageLocation.split("@", 2);
        String containerName = blobLocationList[0];
        String accountName = blobLocationList[1].split("\\.")[0];

        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, this.accessKeyID);
        String endpoint = String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName);

        BlobServiceClient storageClient = new BlobServiceClientBuilder().endpoint(endpoint).credential(credential).buildClient();
        BlobContainerClient blobContainerClient = storageClient.getBlobContainerClient(containerName);

        ListBlobsOptions options = new ListBlobsOptions().setPrefix("ranger/audit/");

        final PagedIterable<BlobItem> blobs = blobContainerClient.listBlobs(options, null);

        for (BlobItem blob : blobs) {
            String[] blobPathList = blob.getName().split("/");
            String potentialDateStr = blobPathList[blobPathList.length - 2];
            String fileName = blobPathList[blobPathList.length - 1];

            if (!isDateStr(potentialDateStr) || !isLaterDate(potentialDateStr, daysAgo)) {
                continue;
            }

            if (Objects.equals(fileName, "")) {
                continue;
            }

            allValidLogPaths.add(blob.getName());
        }
        return allValidLogPaths;
    }

    @Override
    public File downloadFromCloud(String blobLogPath, String localDir) throws IOException {
        /**
         * Download logs from AZURE Blob.
         *
         * @param blobLogPath The log path on Azure blob, for example.
         * @param localDir The local location where we want to store the downloaded files temporarily, this defaults to "tmp_logs".
         * @throws IOException If an I/O error occurs.
         */
        File localFilePath;
        String[] blobLocationList = this.storageLocation.split("@", 2);
        String containerName = blobLocationList[0];
        String accountName = blobLocationList[1].split("\\.")[0];

        // TODO you build the client every time for every file. This is also slow because every time you authenticate it. Create the client inside constructor and re-use it as a field
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, this.accessKeyID);
        String endpoint = String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName);

        BlobServiceClient storageClient = new BlobServiceClientBuilder().endpoint(endpoint).credential(credential).buildClient();
        BlobContainerClient blobContainerClient = storageClient.getBlobContainerClient(containerName);

        BlockBlobClient blobClient = blobContainerClient.getBlobClient(blobLogPath).getBlockBlobClient();
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
}