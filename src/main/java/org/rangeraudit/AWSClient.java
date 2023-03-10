package org.rangeraudit;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.rangeraudit.Utilities.isDateStr;
import static org.rangeraudit.Utilities.isLaterDate;

public class AWSClient {
    private static final Logger LOG = LoggerFactory.getLogger(AWSClient.class);
    /**
     * The s3 storage location where the data is stored, without the prefix. (example: my-bucket-name/my-env-name/data)
     */
    private final String storageLocation;
    /**
     * AWS Access Key ID.
     */
    private final String accessKeyID;
    /**
     * AWS Access Secret Key.
     */
    private final String secretKeyId;

    public AWSClient(String storageLocation, String accessKeyID, String secretKeyId) {
        this.storageLocation = storageLocation;
        this.accessKeyID = accessKeyID;
        this.secretKeyId = secretKeyId;
    }

    public void downloadFromCloud(int daysAgo, String localDir) throws IOException {
        /**
         * Download logs from AWS S3.
         *
         * @param daysAgo How many days ago we want to start downloading the logs, for exmaple, put "0" will download
         * today's logs, and put "2" will download the logs of today, yesterday, and the day before yesterday's.
         * @param localDir The local location where we want to store the downloaded files temporarily, this defaults to "tmp_logs".
         * @throws IOException If an I/O error occurs.
         */
        String[] s3LocationList = this.storageLocation.split("/", 2);
        String bucketName = s3LocationList[0];
        String s3Path = s3LocationList[1];

        Regions clientRegion = Regions.DEFAULT_REGION;
        BasicAWSCredentials credentials = new BasicAWSCredentials(this.accessKeyID, this.secretKeyId);

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(clientRegion)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();

        ObjectListing listing = s3Client.listObjects(bucketName, s3Path);
        List<S3ObjectSummary> summaries = listing.getObjectSummaries();

        for (S3ObjectSummary obj : summaries) {
            if (!obj.getKey().contains("/ranger/audit")) {
                continue;
            }

            String[] s3PathList = obj.getKey().split("/");
            String potentialDateStr = s3PathList[s3PathList.length - 2];
            String fileName = s3PathList[s3PathList.length - 1];

            if (!isDateStr(potentialDateStr) || !isLaterDate(potentialDateStr, daysAgo)) {
                continue;
            }

            if (Objects.equals(fileName, "")) {
                continue;
            }

            // Create /tmp_logs/yyyymmdd directory to store logs.
            File tmpFolder = new File(localDir);
            tmpFolder.mkdir();
            File dateFolder = new File(tmpFolder + "/" + potentialDateStr);
            dateFolder.mkdir();
            File filePath = new File(dateFolder + "/" + fileName);
            filePath.createNewFile();
            try {
                s3Client.getObject(new GetObjectRequest(bucketName, obj.getKey()), filePath);
                LOG.info("Downloaded log: " + filePath);
            } catch (Exception e) {
                LOG.info("Failed at downloading: " + filePath);
                e.printStackTrace();
            }

        }

    }

}



