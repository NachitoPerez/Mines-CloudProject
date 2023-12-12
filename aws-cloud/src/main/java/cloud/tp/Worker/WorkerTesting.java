package cloud.tp.Worker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
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
        Map<String,Map<String, ProductSummaryData>> productSummaries = new HashMap<>();

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

                
                if (downloadedFileName != null){
                    // Initialize the map if not already initialized
                    if (storeSummaries == null) {
                        storeSummaries = new HashMap<>();
                    }

                    // Summarize by store
                    WorkerFunctions.summarizeSalesByStore(storeSummaries, downloadedFileName);
                    // Summarize by product
                    WorkerFunctions.summarizeSalesByProduct(productSummaries, downloadedFileName);
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
            Map<String, List<String>> fileNamesCreated = WorkerFunctions.createResumeFiles(storeSummaries, productSummaries);
            WorkerFunctions.uploadResumeFilesToS3(fileNamesCreated);
        }

        // Close the SQS client
        sqsClient.close();
    }
}
