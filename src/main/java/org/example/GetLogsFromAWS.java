package org.example;

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

import java.io.File;
import java.io.IOException;
import java.util.List;

public class GetLogsFromAWS {

    public static void main(String[] args) throws IOException {
        Regions clientRegion = Regions.DEFAULT_REGION;
        String bucketName = "eng-sdx-daily-v2-datalake";
        String access_key_id = "";
        String secret_key_id = "";

        BasicAWSCredentials credentials = new BasicAWSCredentials(access_key_id, secret_key_id);

        S3Object fullObject = null, objectPortion = null, headerOverrideObject = null;
        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .build();

            ObjectListing listing = s3Client.listObjects(bucketName, "gracezhu-aws-env");
            List<S3ObjectSummary> summaries = listing.getObjectSummaries();

            for (S3ObjectSummary obj:summaries) {
                if (obj.getKey().contains("/ranger/audit")) {
                    System.out.println("Downloading an object:" + obj.getKey());
                    File localFile = new File(obj.getKey());
                    System.out.println(localFile);
                    s3Client.getObject(new GetObjectRequest(bucketName, obj.getKey()), localFile);
                }
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
