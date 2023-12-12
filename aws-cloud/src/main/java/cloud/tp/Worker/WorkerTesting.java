package cloud.tp.Worker;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import cloud.tp.Constants;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

public class WorkerTesting {
    public static void main(String[] args) {
        Region region = Region.US_EAST_1;

        if (args.length < 1) {
            System.out.println("Missing the Queue URL argument");
            System.exit(1);
        }

        String queueURL = args[0];
        Map<String, Map<String, Double>> storeSummaries = new HashMap<>();
        //TODO: implement products summarization
        //Map<String, Quartet<String, Integer, Double, Double>> productSummaries = new HashMap<>();

        SqsClient sqsClient = SqsClient.builder().region(region).build();

        // Receive messages from the queue
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueURL)
                .maxNumberOfMessages(5)
                .build();

        ReceiveMessageResponse receiveResponse = sqsClient.receiveMessage(receiveRequest);
        List<Message> messages = receiveResponse.messages();

        while (!messages.isEmpty()) {
            // Process the first message
            Message message = messages.get(0);
            String[] messageParts = message.body().split(";");
            if (messageParts.length == 2) {
                String bucketName = messageParts[0];
                String fileName = messageParts[1];

                // Download the file from S3
                String downloadedFileName = WorkerFunctions.downloadFileFromS3(bucketName, fileName);

                //summarize by store
                if (downloadedFileName != null){
                    // Initialize the map if not already initialized
                    if (storeSummaries == null) {
                        storeSummaries = new HashMap<>();
                    }

                    // Summarize by store
                    WorkerFunctions.summarizeSalesByStore(storeSummaries, downloadedFileName);
                }

                // Delete the processed message from the queue
                String receiptHandle = message.receiptHandle();
                sqsClient.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(queueURL)
                        .receiptHandle(receiptHandle)
                        .build());

                receiveResponse = sqsClient.receiveMessage(receiveRequest);
                messages = receiveResponse.messages();
            } else {
                System.out.println("Invalid message format: " + message.body());
            }
        } 

        if (!storeSummaries.isEmpty()) {
            List<String> fileNamesCreated = WorkerFunctions.createStoreResumeFiles(storeSummaries);
            WorkerFunctions.uploadProductsResumeFileToS3(fileNamesCreated);
        }

        // Close the SQS client
        sqsClient.close();
    }
}
