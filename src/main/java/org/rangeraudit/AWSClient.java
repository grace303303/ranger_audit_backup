package org.rangeraudit;

import java.io.File;
import java.io.IOException;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.rangeraudit.Utilities.isDateStr;
import static org.rangeraudit.Utilities.isLaterDate;

public class AWSClient {
    /**
     * The s3 storage location where the data is stored, without the prefix. (example: my-bucket-name/my-env-name/data)
     */
    private String storageLocation;
    /**
     * AWS Access Key ID.
     */
    private String accessKeyID;
    /**
     * AWS Access Secret Key.
     */
    private String secretKeyId;

    private static final Logger LOG = LoggerFactory.getLogger(AWSClient.class);

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

        S3Object fullObject = null, objectPortion = null, headerOverrideObject = null;

        try {
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
                s3Client.getObject(new GetObjectRequest(bucketName, obj.getKey()), filePath);
                LOG.info("Downloaded log: " + filePath);

            }

        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        } finally {
            // To ensure that the network connection doesn't remain open, close any open input streams.
            if (fullObject != null) {
                fullObject.close();
            }
            if (objectPortion != null) {
                objectPortion.close();
            }
            if (headerOverrideObject != null) {
                headerOverrideObject.close();
            }
        }

    }


}
