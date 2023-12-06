package cloud.tp.emseCourse.s3;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.nio.file.Path;

public class S3FileOperations {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java S3FileOperations <bucket_name>");
            System.exit(1);
        }

        String bucketName = args[0];

        // Create an S3 client
        S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();

        // Check if the file exists in the bucket
        if (doesFileExist(s3, bucketName, "values.csv")) {
            // If the file exists, retrieve and delete it
            retrieveFile(s3, bucketName, "values.csv", "downloaded_values.csv");
            deleteFile(s3, bucketName, "values.csv");
        } else {
            System.out.println("File values.csv does not exist in the bucket.");
        }

        // Close the S3 client
        s3.close();
    }

    private static boolean doesFileExist(S3Client s3, String bucketName, String key) {
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        try {
            // If the head object request succeeds, the file exists
            s3.headObject(headObjectRequest);
            return true;
        } catch (S3Exception e) {
            // If the head object request fails, the file does not exist
            if (e.statusCode() == 404) {
                return false;
            } else {
                throw e;
            }
        }
    }

    private static void retrieveFile(S3Client s3, String bucketName, String key, String destinationPath) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        s3.getObject(getObjectRequest, Path.of(destinationPath));
        System.out.println("File retrieved from S3 bucket: " + bucketName);
    }

    private static void deleteFile(S3Client s3, String bucketName, String key) {
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        s3.deleteObject(deleteObjectRequest);
        System.out.println("File deleted from S3 bucket: " + bucketName);
    }
}
