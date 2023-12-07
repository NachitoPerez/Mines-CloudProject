package cloud.tp;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class ClientApp {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: java S3BucketOperations <file_path>");
            System.exit(1);
        }

        String filePath = args[0];

        // Create an S3 client
        S3Client s3 = S3Client.builder().region(Constants.REGION).build();

        // Check if the bucket exists
        if (!doesBucketExist(s3)) {
            throw new Exception("Bucket not correct");
        }

        // Change name file from {date}-store{number}.csv to store{number}/{date}.csv
        String newName = modifyFileName(filePath);

        // Upload the file to the bucket
        uploadFile(s3, newName, new File(filePath));

        // Send SQS msg
        sendSQSmsg(newName);
        
        // Close the s3 client
        s3.close();
    }

    private static boolean doesBucketExist(S3Client s3) {
        HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                .bucket(Constants.BUCKET_NAME)
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

    private static void uploadFile(S3Client s3, String key, File file) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(Constants.BUCKET_NAME)
                .key(key)
                .build();

        s3.putObject(putObjectRequest, file.toPath());
        System.out.println("File " + key + " uploaded to S3 bucket: " + Constants.BUCKET_NAME);
    }

    private static void sendSQSmsg(String fileName){
        SqsClient sqsClient = SqsClient.builder().region(Constants.REGION).build();
        SendMessageRequest sendRequest = SendMessageRequest.builder().queueUrl(Constants.QUEUE_URL)
            .messageBody(Constants.BUCKET_NAME + ";" + fileName).build();

        SendMessageResponse sqsResponse = sqsClient.sendMessage(sendRequest);

        System.out.println(
            sqsResponse.messageId() + " Message "+ Constants.BUCKET_NAME + ";" + fileName +" sent. Status is " + sqsResponse.sdkHttpResponse().statusCode());
        
        sqsClient.close();
    }
}
