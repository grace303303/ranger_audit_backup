package org.example;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobContainerClient;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Iterator;
import java.util.Locale;

public class GetLogsFromAzure {

    /**
     * Entry point into the basic examples for Storage blobs.
     *
     * @param args Unused. Arguments to the program.
     * @throws IOException      If an I/O error occurs
     * @throws RuntimeException If the downloaded data doesn't match the uploaded data
     */
    public static void main(String[] args) throws IOException {

        String accountName = "gracezhuresourcegroupsan";
        String accountKey = "";
        String containerName = "data";

        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        String endpoint = String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName);
        BlobServiceClient storageClient = new BlobServiceClientBuilder().endpoint(endpoint).credential(credential).buildClient();
        BlobContainerClient blobContainerClient = storageClient.getBlobContainerClient(containerName);

        ListBlobsOptions options = new ListBlobsOptions()
                .setMaxResultsPerPage(10)
                .setDetails(new BlobListDetails().setRetrieveDeletedBlobs(false).setRetrieveSnapshots(true))
                .setPrefix("ranger/audit/hdfs/");
        Duration duration = Duration.ofMinutes(3);

        final Iterator<BlobItem> result = blobContainerClient.listBlobs(options, null).iterator();

        while (result.hasNext()) {

            final BlobItem blob = result.next();
            System.out.println("\t" + blob.getName());

        }

        System.out.println("end");

//        BlockBlobClient blobClient = blobContainerClient.getBlobClient("ranger/audit/hdfs/hdfs/20230105/hdfs_ranger_audit_gracezhu-azure-env-master0.gracezhu.xcu2-8y8x.wl.cloudera.site.log").getBlockBlobClient();
//        System.out.println((int) blobClient.getProperties().getBlobSize());

    }

}