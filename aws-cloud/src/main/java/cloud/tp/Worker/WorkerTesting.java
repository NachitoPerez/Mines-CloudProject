package cloud.tp.Worker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
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
import org.javatuples.Quartet;

import cloud.tp.Constants;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
// import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
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

                System.out.println("Message content: " + bucketName + ";" + fileName);

                // Download the file from S3
                String downloadedFileName = downloadFileFromS3(bucketName, fileName);

                //summarize by store
                if (downloadedFileName != null){
                    // Initialize the map if not already initialized
                    if (storeSummaries == null) {
                        storeSummaries = new HashMap<>();
                    }

                    // Summarize by store
                    Map<String, Map<String, Double>> moreStoreSummaries = summarizeSalesByStore(downloadedFileName);

                    for (String key : moreStoreSummaries.keySet()) {
                        if (storeSummaries.containsKey(key)) {
                            // If the key already exists, update the existing entry
                            storeSummaries.get(key).putAll(moreStoreSummaries.get(key));
                        } else {
                            // If the key does not exist, add a new entry
                            storeSummaries.put(key, new HashMap<>(moreStoreSummaries.get(key)));
                        }
                    }
                    
                    System.out.println("array: " + storeSummaries);
                    //createStoresResumeFile(storeSummaries);

                }

                receiveResponse = sqsClient.receiveMessage(receiveRequest);
                messages = receiveResponse.messages();


                // Delete the processed message from the queue
                // String receiptHandle = message.receiptHandle();
                // sqsClient.deleteMessage(DeleteMessageRequest.builder()
                //         .queueUrl(queueURL)
                //         .receiptHandle(receiptHandle)
                //         .build());
            } else {
                System.out.println("Invalid message format: " + message.body());
            }
        } 

        if (!storeSummaries.isEmpty()) {
            List<String> fileNamesCreated = createProductsResumeFiles(storeSummaries);
            uploadProductsResumeFileToS3(fileNamesCreated);
        }

        // Close the SQS client
        sqsClient.close();
    }

    private static String downloadFileFromS3(String bucketName, String fileName) {
        S3Client s3Client = S3Client.builder().region(Region.US_EAST_1).build();

        // Check file exists
        ListObjectsRequest listObjects = ListObjectsRequest.builder().bucket(bucketName).build();

        ListObjectsResponse res = s3Client.listObjects(listObjects);
        List<S3Object> objects = res.contents();

        if (objects.stream().anyMatch((S3Object x) -> x.key().equals(fileName))) {

            // Retrieve file
            GetObjectRequest objectRequest = GetObjectRequest.builder().key(fileName).bucket(bucketName).build();

            ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(objectRequest);
            byte[] data = objectBytes.asByteArray();

            File file = new File("fileToProcess.csv");
            try (OutputStream os = new FileOutputStream(file)) {
                os.write(data);
            } catch (IOException e) {
                e.printStackTrace();
            }

            return "fileToProcess.csv";
        } else {
            System.out.println("File is not available in the Bucket");
        }
            
        s3Client.close();
        return null;
    }

    private static List<Map<String, String>> parseCSVFile(String fileName) {
        List<Map<String, String>> csvData = new ArrayList<>();

        try (Reader reader = new FileReader(new File(fileName));
             CSVParser csvParser = CSVFormat.DEFAULT.withDelimiter(';').withFirstRecordAsHeader().parse(reader)) {

            for (CSVRecord csvRecord : csvParser) {
                // Each CSVRecord is a map where the keys are the header column names
                // and values are the corresponding values in the current row
                csvData.add(csvRecord.toMap());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return csvData;
    }

    private static String extractDate(String dateTime) {
        try {
            Date date = new SimpleDateFormat("dd/MM/yyyy HH:mm").parse(dateTime);
            return new SimpleDateFormat("ddMMyyyy").format(date);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static Map<String, Map<String, Double>> summarizeSalesByStore(String fileName) {
        // Load the CSV file and perform the summarization
        // You can use a CSV parsing library for a more robust solution

        // For simplicity, let's assume you have a method to parse the CSV file and
        // return a list of Map<String, String> representing each row
        List<Map<String, String>> csvData = parseCSVFile(fileName);

        // Summarize sales by date and store
        return csvData.stream()
        .collect(Collectors.groupingBy(row -> extractDate(row.get("Date_Time")),
                Collectors.groupingBy(row -> row.get("Store"),
                        Collectors.summingDouble(row -> Double.parseDouble(row.get("Unit_Profit"))))));
    }

    private static List<String> createProductsResumeFiles(Map<String, Map<String, Double>> storeSummaries) {
        List<String> fileNames = new ArrayList<>();

        for (Map.Entry<String, Map<String, Double>> dateEntry : storeSummaries.entrySet()) {
            String date = dateEntry.getKey();
            Map<String, Double> storeProfits = dateEntry.getValue();

            // Create the file name with the current date
            String productsResumeFileName = date + "-products-resume.csv";

            // Write the summaries to the products resume file
            try (OutputStream os = new FileOutputStream(productsResumeFileName)) {
                // Write header
                os.write("Store;TotalDailyProfit".getBytes());

                for (Map.Entry<String, Double> storeEntry : storeProfits.entrySet()) {
                    String store = storeEntry.getKey();
                    double totalDailyProfit = storeEntry.getValue();

                    // Write the data to the file
                    String line = System.lineSeparator() + store + ";" + totalDailyProfit;
                    os.write(line.getBytes());
                }

                System.out.println("Products resume file created: " + productsResumeFileName);
                fileNames.add(productsResumeFileName);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return fileNames;
    }

    private static void uploadProductsResumeFileToS3(List<String> fileNames) {
        //TODO: check if file already exists
            // in case it exists, add the information
        S3Client s3Client = S3Client.builder().region(Region.US_EAST_1).build();

        try {
            for (String fileName : fileNames) {
                File fileToUpload = new File(fileName);
                String key = "storeResumes/" + fileName;

                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                        .bucket(Constants.BUCKET_NAME)
                        .key(key)
                        .build();

                s3Client.putObject(putObjectRequest, fileToUpload.toPath());

                System.out.println("File uploaded to S3 bucket: " + Constants.BUCKET_NAME + ", Key: " + key);
            }
        } catch (S3Exception e) {
            e.printStackTrace();
        } finally {
            // Close the S3 client
            s3Client.close();
        }
    }
}
