package fr.emse.sqs;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.nio.file.Path;
import java.util.List;

public class ReceiveAndDownloadMessage {

    public static void main(String[] args) {
        Region region = Region.US_EAST_1;

        if (args.length < 1) {
            System.out.println("Missing the Queue URL argument");
            System.exit(1);
        }

        String queueURL = args[0];

        SqsClient sqsClient = SqsClient.builder().region(region).build();

        // Receive messages from the queue
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueURL)
                .maxNumberOfMessages(1)
                .build();

        ReceiveMessageResponse receiveResponse = sqsClient.receiveMessage(receiveRequest);
        List<Message> messages = receiveResponse.messages();

        if (!messages.isEmpty()) {
            // Process the first message
            Message message = messages.get(0);
            String[] messageParts = message.body().split(";");
            if (messageParts.length == 2) {
                String bucketName = messageParts[0];
                String fileName = messageParts[1];

                // Download the file from S3
                downloadFileFromS3(bucketName, fileName);

                // Delete the processed message from the queue
                String receiptHandle = message.receiptHandle();
                sqsClient.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(queueURL)
                        .receiptHandle(receiptHandle)
                        .build());
            } else {
                System.out.println("Invalid message format: " + message.body());
            }
        } else {
            System.out.println("No messages in the queue.");
        }

        // Close the SQS client
        sqsClient.close();
    }

    private static void downloadFileFromS3(String bucketName, String fileName) {
        S3Client s3Client = S3Client.builder().region(Region.US_EAST_1).build();

        try {
            // Download the file from S3
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fileName)
                    .build();

            s3Client.getObject(getObjectRequest, Path.of(fileName));
            System.out.println("File downloaded from S3: " + fileName);
        } catch (S3Exception e) {
            System.out.println("Error downloading file from S3: " + e.getMessage());
        } finally {
            // Close the S3 client
            s3Client.close();
        }
    }
}
