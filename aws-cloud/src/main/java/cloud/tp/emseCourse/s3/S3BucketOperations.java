package fr.emse.s3;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;

public class S3BucketOperations {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: java S3BucketOperations <bucket_name> <file_path>");
            System.exit(1);
        }

        String bucketName = args[0];
        String filePath = args[1];

        // Create an S3 client
        S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();

        // Check if the bucket exists
        if (!doesBucketExist(s3, bucketName)) {
            // If the bucket doesn't exist, create it
            createBucket(s3, bucketName);
        }

        // Upload the file to the bucket
        uploadFile(s3, bucketName, "values.csv", new File(filePath));

        // Close the S3 client
        s3.close();
    }

    private static boolean doesBucketExist(S3Client s3, String bucketName) {
        HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                .bucket(bucketName)
                .build();

        try {
            // If the head bucket request succeeds, the bucket exists
            s3.headBucket(headBucketRequest);
            return true;
        } catch (S3Exception e) {
            // If the head bucket request fails, the bucket does not exist
            if (e.statusCode() == 404) {
                return false;
            } else {
                throw e;
            }
        }
    }

    private static void createBucket(S3Client s3, String bucketName) {
        CreateBucketRequest createBucketRequest = CreateBucketRequest.builder()
                .bucket(bucketName)
                .build();

        s3.createBucket(createBucketRequest);
        System.out.println("Bucket created: " + bucketName);
    }

    private static void uploadFile(S3Client s3, String bucketName, String key, File file) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        s3.putObject(putObjectRequest, file.toPath());
        System.out.println("File uploaded to S3 bucket: " + bucketName);
    }   
}

