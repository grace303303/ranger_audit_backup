package org.example;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobContainerClient;

import java.io.File;
import java.io.IOException;
import java.util.Locale;

import static org.example.Utilities.isDateStr;
import static org.example.Utilities.isLaterDate;

public class AzureClient {

    private String storageLocation;
    private String accessKeyID;

    public AzureClient(String storageLocation, String accessKeyID) {
        this.storageLocation = storageLocation;
        this.accessKeyID = accessKeyID;
    }

    public void downloadFromCloud(String localDir, int daysAgo) throws IOException {
        /**
         * Download logs from AZURE.
         *
         * @param args Unused. Arguments to the program.
         * @throws IOException      If an I/O error occurs
         * @throws RuntimeException If the downloaded data doesn't match the uploaded data
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
            File dateFolder = new File(potentialDateStr);
            dateFolder.mkdir();
            File filePath = new File(localDir + "/" +  potentialDateStr + "/" + blob.getName());
            filePath.createNewFile();
            blobClient.downloadToFile(filePath.toString(), true);
            System.out.println("Downloaded " + blob.getName() + "to: " + filePath.toString());


        }


    }


}