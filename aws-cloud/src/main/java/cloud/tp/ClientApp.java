package main.java.cloud.tp;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.File;

public class ClientApp {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: java S3BucketOperations <file_path>");
            System.exit(1);
        }

        String bucketName = "cloudprojecttest";
        String filePath = args[0];

        // Create an S3 client
        S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();

        // Check if the bucket exists
        if (!doesBucketExist(s3, bucketName)) {
            throw new Exception("Bucket not correct");
        }

        //Change name file from {date}-store{number}.csv to store{number}/{date}.csv
        String newName = modifyFileName(filePath);

        // Upload the file to the bucket
        uploadFile(s3, bucketName, newName, new File(filePath));

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

    private static String modifyFileName(String filePath) throws Exception {
        // Define the regex pattern to match the original format
        Pattern pattern = Pattern.compile("(\\d{2}-\\d{2}-\\d{4})-store(\\d+)(\\.\\w+)");
        
        // Create a matcher with the input filepath
        Matcher matcher = pattern.matcher(filePath);
        
        // Check if the pattern matches
        if (matcher.find()) {
            // Extract matched groups
            String datePart = matcher.group(1);
            String storeNumber = matcher.group(2);
            String fileExtension = matcher.group(3);

            // Create the modified file name
            String modifiedFileName = "storesDaily/" + datePart + "-store" + storeNumber + fileExtension;
            
            return modifiedFileName;
        } else {
            // If the pattern doesn't match, return the original filename
            throw new Exception("Invalid file name. File must follow the syntax: DD-MM-YYYY-store{store_number}.csv");
        }
    }

    private static void uploadFile(S3Client s3, String bucketName, String key, File file) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        s3.putObject(putObjectRequest, file.toPath());
        System.out.println("File " + key + " uploaded to S3 bucket: " + bucketName);
    }  
}
