// Copyright (c) 2023 Cloudera, Inc. All rights reserved.
package org.rangeraudit;

import static org.rangeraudit.Utilities.isDateStr;
import static org.rangeraudit.Utilities.isLaterDate;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class AWSClient implements CloudClient {
    /**
     * The AWS Client used to run commands for AWS.
     */
    private static final Logger LOG = LoggerFactory.getLogger(AWSClient.class);
    private final String storageLocation;

    private final Regions clientRegion;
    private final BasicAWSCredentials credentials;

    public AWSClient(String storageLocation, String accessKeyID,
            String secretKeyId, String region) {
        this.storageLocation = storageLocation;
        this.clientRegion = Regions.fromName(region);
        this.credentials = new BasicAWSCredentials(accessKeyID, secretKeyId);
    }

    /**
     * Get all log paths from S3 that match requirements: within the date,
     * it is a ranger audit file etc.
     *
     * @param daysAgo How many days ago we want to start downloading the logs,
     *                for example, put "0" will download today's logs,
     *                and put "2" will download the logs of today, yesterday,
     *                and the day before yesterday's.
     * @return An ArrayList of all the valid s3 log path.
     */
    @Override
    public ArrayList<String> getAllValidLogPaths(int daysAgo) {
        ArrayList<String> allValidLogPaths = new ArrayList();
        String[] s3LocationList = storageLocation.split("/", 2);
        String bucketName = s3LocationList[0];
        String s3Path = s3LocationList[1];

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
            if (!isDateStr(potentialDateStr)
                    || !isLaterDate(potentialDateStr, daysAgo)) {
                continue;
            }
            if (Objects.equals(fileName, "")) {
                continue;
            }
            allValidLogPaths.add(obj.getKey());
        }
        return allValidLogPaths;
    }

    /**
     * Download logs from AWS S3.
     *
     * @param s3logPath The log path on AWS S3, for example,
     *                  s3a://my-bucket/test/
     * @param localDir The local location where we want to store the downloaded
     *                 files temporarily, this defaults to "tmp_logs".
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public File downloadFromCloud(String s3logPath, String localDir)
            throws IOException {
        File localFilePath;
        String[] s3LocationList = storageLocation.split("/", 2);
        String bucketName = s3LocationList[0];

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(clientRegion)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();

        String[] s3PathList = s3logPath.split("/");
        String potentialDateStr = s3PathList[s3PathList.length - 2];
        String fileName = s3PathList[s3PathList.length - 1];

        // Create /tmp_logs/yyyymmdd directory to store logs.
        File tmpFolder = new File(localDir);
        tmpFolder.mkdir();
        File dateFolder = new File(tmpFolder + "/" + potentialDateStr);
        dateFolder.mkdir();
        localFilePath = new File(dateFolder + "/" + fileName);
        localFilePath.createNewFile();
        try {
            s3Client.getObject(new GetObjectRequest(bucketName, s3logPath),
                    localFilePath);
            LOG.info("Downloaded log to: " + localFilePath);
        } catch (Exception e) {
            LOG.info("Failed at downloading: " + s3logPath);
            e.printStackTrace();
            return null;
        }
        return localFilePath;
    }
}



