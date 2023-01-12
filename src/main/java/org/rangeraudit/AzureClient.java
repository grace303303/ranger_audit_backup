package org.rangeraudit;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Locale;

import static org.rangeraudit.Utilities.isDateStr;
import static org.rangeraudit.Utilities.isLaterDate;

public class AzureClient {
    /**
     *The blob storage location where the data is stored, without the prefix.
     *(example: data@myresourcegroup.dfs.core.windows.net )
     */
    private String storageLocation;
    /**
     *The Access Key of the storage account.
     */
    private String accessKeyID;
    private static final Logger LOG = LoggerFactory.getLogger(AzureClient.class);

    public AzureClient(String storageLocation, String accessKeyID) {

        this.storageLocation = storageLocation;
        this.accessKeyID = accessKeyID;
    }

    public void downloadFromCloud(int daysAgo, String localDir) throws IOException {
        /**
         * Download logs from AZURE Blob.
         *
         * @param daysAgo How many days ago we want to start downloading the logs, for exmaple, put "0" will download
         * today's logs, and put "2" will download the logs of today, yesterday, and the day before yesterday's.
         * @param localDir The local location where we want to store the downloaded files temporarily, this defaults to "tmp_logs".
         * @throws IOException If an I/O error occurs.
         */

        String[] blobLocationList = this.storageLocation.split("@", 2);
        String containerName = blobLocationList[0];
        String accountName = blobLocationList[1].split("\\.")[0];

        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, this.accessKeyID);
        String endpoint = String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName);

        BlobServiceClient storageClient = new BlobServiceClientBuilder().endpoint(endpoint).credential(credential).buildClient();
        BlobContainerClient blobContainerClient = storageClient.getBlobContainerClient(containerName);

        ListBlobsOptions options = new ListBlobsOptions().setPrefix("ranger/audit/");

        final PagedIterable<BlobItem> blobs = blobContainerClient.listBlobs(options, null);

        for (BlobItem blob: blobs) {
            BlockBlobClient blobClient = blobContainerClient.getBlobClient(blob.getName()).getBlockBlobClient();
            String[] blobPathList = blob.getName().split("/");
            String potentialDateStr = blobPathList[blobPathList.length - 2];
            String fileName = blobPathList[blobPathList.length - 1];

            if (!isDateStr(potentialDateStr) || !isLaterDate(potentialDateStr, daysAgo)) {
                continue;
            }

            if (fileName == "") {
                continue;
            }

            // Create /tmp_logs/yyyymmdd directory to store logs.
            File tmpFolder = new File(localDir);
            tmpFolder.mkdir();
            File dateFolder = new File(tmpFolder + "/" + potentialDateStr);
            dateFolder.mkdir();
            File filePath = new File(dateFolder + "/" + fileName);
            filePath.createNewFile();
            blobClient.downloadToFile(filePath.toString(), true);
            LOG.info("Downloaded log: " + filePath);

        }


    }


}