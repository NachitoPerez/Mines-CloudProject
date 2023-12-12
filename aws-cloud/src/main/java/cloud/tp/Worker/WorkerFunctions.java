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
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

public class WorkerFunctions {
    public static String downloadFileFromS3(String bucketName, String fileName) {
        S3Client s3Client = S3Client.builder().region(Region.US_EAST_1).build();
        String response = null;

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

            response = "fileToProcess.csv";
        }
            
        s3Client.close();
        return response;
    }

    private static List<Map<String, String>> parseCSVFile(String fileName) {
        List<Map<String, String>> csvData = new ArrayList<>();

        try (Reader reader = new FileReader(new File(fileName));
             CSVParser csvParser = CSVFormat.DEFAULT.withDelimiter(';').withFirstRecordAsHeader().parse(reader)) {

            for (CSVRecord csvRecord : csvParser) {
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

    public static void summarizeSalesByStore(Map<String, Map<String, Double>> storeSummaries, String downloadedFileName){
        Map<String, Map<String, Double>> moreStoreSummaries = summarizeMyFileByStore(downloadedFileName);

        for (String key : moreStoreSummaries.keySet()) {
            if (storeSummaries.containsKey(key)) {
                // If the key already exists, update the existing entry
                storeSummaries.get(key).putAll(moreStoreSummaries.get(key));
            } else {
                // If the key does not exist, add a new entry
                storeSummaries.put(key, new HashMap<>(moreStoreSummaries.get(key)));
            }
        }
    }

    private static Map<String, Map<String, Double>> summarizeMyFileByStore(String fileName) {
        List<Map<String, String>> csvData = parseCSVFile(fileName);

        // Summarize sales by date and store
        return csvData.stream()
        .collect(Collectors.groupingBy(row -> extractDate(row.get("Date_Time")),
                Collectors.groupingBy(row -> row.get("Store"),
                        Collectors.summingDouble(row -> Double.parseDouble(row.get("Unit_Profit"))))));
    }

    public static List<String> createStoreResumeFiles(Map<String, Map<String, Double>> storeSummaries) {
        List<String> fileNames = new ArrayList<>();

        for (Map.Entry<String, Map<String, Double>> dateEntry : storeSummaries.entrySet()) {
            String date = dateEntry.getKey();
            Map<String, Double> storeProfits = dateEntry.getValue();

            // Create the file name with the current date
            String storesResumeFileName = date + "-stores-resume.csv";

            // Write the summaries to the products resume file
            try (OutputStream os = new FileOutputStream(storesResumeFileName)) {
                // Write header
                os.write("Store;TotalDailyProfit".getBytes());

                for (Map.Entry<String, Double> storeEntry : storeProfits.entrySet()) {
                    String store = storeEntry.getKey();
                    double totalDailyProfit = storeEntry.getValue();

                    // Write the data to the file
                    String line = System.lineSeparator() + store + ";" + totalDailyProfit;
                    os.write(line.getBytes());
                }

                fileNames.add(storesResumeFileName);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return fileNames;
    }

    public static void uploadResumeFilesToS3(Map<String, List<String>> fileNames) {
        S3Client s3Client = S3Client.builder().region(Region.US_EAST_1).build();

        try {
            for (Map.Entry<String, List<String>> fileName : fileNames.entrySet()) {
                String directory = fileName.getKey();

                List<String> files2Upload = fileName.getValue();

                for (String file : files2Upload) {
                    // Check if the object already exists in the bucket
                    String existingFileName = downloadFileFromS3(Constants.BUCKET_NAME, directory+"/"+file);

                    if (existingFileName != null) {
                        // Object exists, read and merge the content with the new file    
                        createMergedFile(existingFileName, file);                        
                    }
                        
                    uploadNewObject(s3Client, Constants.BUCKET_NAME, directory + "/" + file, new File(file));

                    System.out.println("File uploaded to S3 bucket: " + Constants.BUCKET_NAME + ", Key: " + file);
                }                
            }

        } catch (S3Exception e) {
            e.printStackTrace();
        } finally {
            // Close the S3 client
            s3Client.close();
        }
    }

    private static void createMergedFile(String existingFileName, String file) {
        // Read the existing content from the existing file
        List<String> existingContent = readContentFromFile(existingFileName);
        
        // Read the new content from the new file
        List<String> newContent = readContentFromFile(file);

        // Merge the content
        List<String> mergedContent = mergeContent(existingContent, newContent);

        // Create file with updated content
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            if (mergedContent.get(0).split(";").length == 2) {
                writer.write("Store;TotalDailyProfit");
            } else if (mergedContent.get(0).split(";").length == 4){
                writer.write("Product;Total quantity;Total sold;Total profit");
            }
            writer.newLine();
            for (String line : mergedContent) {
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<String> readContentFromFile(String fileName) 
    {
        // Implement logic to read the content of the file
        // You may use FileReader, BufferedReader, or other approaches

        List<String> content = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            // Skip the header line
            reader.readLine();

            String line;
            while ((line = reader.readLine()) != null) {
                content.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return content;
    }

    private static List<String> mergeContent(List<String> existingContent, List<String> newContent) {

        Map<String, Double> storeProfitMap = new HashMap<>();
        Map<String, ProductSummaryData> productMap = new HashMap<>();

        // Populate storeProfitMap with existing content
        for (String line : existingContent) {
            String[] parts = line.split(";");
            if (parts.length == 2) {
                String store = parts[0];
                double totalProfit = Double.parseDouble(parts[1]);
                storeProfitMap.put(store, totalProfit);
            } else if (parts.length == 4) {
                String product = parts[0];
                int quantity = Integer.parseInt(parts[1]);
                double sold = Double.parseDouble(parts[2]);
                double profit = Double.parseDouble(parts[3]);

                productMap.put(product, new ProductSummaryData(quantity, sold, profit));
            }
        }

        // Update storeProfitMap with new content
        for (String line : newContent) {
            String[] parts = line.split(";");
            if (parts.length == 2) {
                String store = parts[0];
                double totalProfit = Double.parseDouble(parts[1]);
                storeProfitMap.merge(store, totalProfit, Double::sum);
            } else if (parts.length == 4) {
                String product = parts[0];
                int quantity = Integer.parseInt(parts[1]);
                double sold = Double.parseDouble(parts[2]);
                double profit = Double.parseDouble(parts[3]);

                ProductSummaryData productSummary = productMap.getOrDefault(product, null);

                if (productSummary != null) {
                    productSummary.update(quantity, sold, profit);
                } else {
                    productMap.put(product, new ProductSummaryData(quantity, sold, profit));
                }
            }
        }

        // Convert storeProfitMap back to a list of strings
        List<String> mergedContent = new ArrayList<>();
        for (Map.Entry<String, Double> entry : storeProfitMap.entrySet()) {
            String line = entry.getKey() + ";" + entry.getValue();
            mergedContent.add(line);
        }

        for (Map.Entry<String, ProductSummaryData> entry : productMap.entrySet()) {
            String key = entry.getKey();
            ProductSummaryData productSummaryData =  entry.getValue();

            String line = key + ";" + productSummaryData.getTotalQuantity() 
                + ";" + productSummaryData.getTotalSold() + ";" + productSummaryData.getTotalProfit();

            mergedContent.add(line);
        }

        return mergedContent;
    }

    private static void uploadNewObject(S3Client s3Client, String bucketName, String key, File file) {
        // Upload the new file to S3
        try {
            s3Client.putObject(PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build(), file.toPath());
        } catch (S3Exception e) {
            e.printStackTrace();
        }
    }

    public static void summarizeSalesByProduct(Map<String, Map<String, ProductSummaryData>> productSummaries,
            String downloadedFileName) {
        // Read the content of the file
        List<String> csvData = readContentFromFile(downloadedFileName);

        // Summarize by product
        for (String line : csvData) {
            String[] columns = line.split(";");
            if (columns.length == 8) { // Assuming there are 8 columns in the CSV
                String date = extractDate(columns[0]);
                String product = columns[2];
                int quantity = Integer.parseInt(columns[3]);
                double totalSold = Double.parseDouble(columns[7]);
                double totalProfit = Double.parseDouble(columns[6]);

                // Update the product summary
                productSummaries
                    .computeIfAbsent(date, k -> new HashMap<>())
                    .compute(product, (key, summary) -> {
                        if (summary == null) {
                            return new ProductSummaryData(quantity, totalSold, totalProfit);
                        } else {
                            summary.update(quantity, totalSold, totalProfit);
                            return summary;
                        }
                    });
            }
        }
    }

    public static Map<String, List<String>> createResumeFiles(Map<String, Map<String, Double>> storeSummaries,
            Map<String, Map<String, ProductSummaryData>> productSummaries) {
        Map<String, List<String>> fileNames = new HashMap<>();

        fileNames.put("storeResumes", createStoreResumeFiles(storeSummaries));
        fileNames.put("productResumes", createProductResumeFiles(productSummaries));

        return fileNames;
    }

    public static List<String> createProductResumeFiles(Map<String, Map<String, ProductSummaryData>> productSummaries) {
        List<String> fileNames = new ArrayList<>();

        for (Map.Entry<String, Map<String, ProductSummaryData>> dateEntry : productSummaries.entrySet()) {
            String date = dateEntry.getKey();
            Map<String, ProductSummaryData> productProfits = dateEntry.getValue();

            // Create the file name with the current date
            String productsResumeFileName = date + "-products-resume.csv";

            // Write the summaries to the products resume file
            try (OutputStream os = new FileOutputStream(productsResumeFileName)) {
                // Write header
                os.write("Product;Total quantity;Total sold;Total profit".getBytes());

                for (Map.Entry<String, ProductSummaryData> productEntry : productProfits.entrySet()) {
                    String product = productEntry.getKey();
                    ProductSummaryData summary = productEntry.getValue();

                    // Write the data to the file
                    String line = System.lineSeparator() + String.format("%s;%d;%.2f;%.2f", product, summary.getTotalQuantity(),
                        summary.getTotalSold(), summary.getTotalProfit());
                    os.write(line.getBytes());
                }

                fileNames.add(productsResumeFileName);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return fileNames;
    }
}
