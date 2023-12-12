package cloud.tp;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;



import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

import java.io.OutputStream;
import java.io.Reader;
import java.util.ArrayList;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ConsolidatorApp {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: ConsolidatorApp <date>");
            System.exit(1);
        }

        String date = args[0];

        // Specify bucket name and file names
        String bucketName = Constants.BUCKET_NAME;
        String productSummaryFileName = date + "-product-summary.csv";
        String storeSummaryFileName = date + "-store-summary.csv";

        String downloadedStoreFileName = downloadFileFromS3(bucketName, storeSummaryFileName);
        String downloadedProductFileName = downloadFileFromS3(bucketName, productSummaryFileName);

        // Perform calculations and display results
        displayResults(downloadedStoreFileName, downloadedProductFileName);
    }

    private static String downloadFileFromS3(String bucketName, String fileName) {
        S3Client s3Client = S3Client.builder().region(Constants.REGION).build();

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




    private static String calculateTotalProfit(String fileName) {
        
       
        List<Map<String, String>> csvData = parseCSVFile(fileName);
        double totalProfit = 0.0;

        for (Map<String, String> row : csvData) {
            if (row.containsKey("Profit")) {
                totalProfit += Double.parseDouble(row.get("Profit"));
            }
        }

        // Format the total profit
        DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance(Locale.US);
        symbols.setDecimalSeparator('.');
        DecimalFormat decimalFormat = new DecimalFormat("$#,##0.0#", symbols);
        return decimalFormat.format(totalProfit);
           
    }

    private static String calculateMaxMinStores(String fileName, Boolean max) {
        
          

        List<Map<String, String>> csvData = parseCSVFile(fileName);
        
        double m = 0.0;        
        String store = "";
        String messageP1="";
        String messageP2="";

        if(max == Boolean.FALSE){
            m = 1000000000;
            messageP1 = "Least profitable store: ";
            messageP2 = " with a total profit of: ";
        }
        else{
            messageP1 = "Most profitable store: ";
            messageP2 = " with a total profit of: ";
        }


        for (Map<String, String> row : csvData) {
            if (row.containsKey("Profit")) {
                if(max==Boolean.FALSE){
                    if(Double.parseDouble(row.get("Profit")) <= m){
                        m = Double.parseDouble(row.get("Profit"));
                        store = row.get("Store");
                    }
                }
                else{
                    if(Double.parseDouble(row.get("Profit")) >= m){
                        m = Double.parseDouble(row.get("Profit"));
                        store = row.get("Store");
                    }
                }
            }
        }

        // Format the total profit
        DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance(Locale.US);
        symbols.setDecimalSeparator('.');
        DecimalFormat decimalFormat = new DecimalFormat("$#,##0.0#", symbols);
        
        return messageP1 + store + messageP2 + decimalFormat.format(m);
           
    }



    private static void displayResults(String storeFileName, String productFileName) {
        // Implement logic to calculate and display the requested results
        // ...

        // Example:
        System.out.println();
        System.out.println("----------------------------------------------------");
        System.out.println();
        System.out.println("Total Profit: " + calculateTotalProfit(storeFileName));
        System.out.println();
        System.out.println(calculateMaxMinStores(storeFileName,Boolean.TRUE));
        System.out.println(calculateMaxMinStores(storeFileName,Boolean.FALSE));
        System.out.println();
        System.out.println("----------------------------------------------------");
        System.out.println();
        // ...
    }


}