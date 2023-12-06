package fr.emse.s3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3RetrieveFile {

  public static void main(String[] args) {
    Region region = Region.US_EAST_1;

    if (args.length < 2) {
      System.out.println("Missing the Bucket Name or File Name arguments");
      System.exit(1);
    }

    String bucketName = args[0];
    String filename = args[1];

    S3Client s3 = S3Client.builder().region(region).build();

    // Check file exists
    ListObjectsRequest listObjects = ListObjectsRequest.builder()
        .bucket(bucketName).build();

    ListObjectsResponse res = s3.listObjects(listObjects);
    List<S3Object> objects = res.contents();

    if (objects.stream().anyMatch((S3Object x) -> x.key().equals(filename))) {

      // Retrieve file
      GetObjectRequest objectRequest = GetObjectRequest.builder().key(filename)
          .bucket(bucketName).build();

      ResponseBytes<
          GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
      byte[] data = objectBytes.asByteArray();

      File file = new File(filename);
      try (OutputStream os = new FileOutputStream(file)) {
        os.write(data);
      } catch (IOException e) {
        e.printStackTrace();
      }

      // Delete file
      ObjectIdentifier objectId = ObjectIdentifier.builder().key(filename)
          .build();

      List<ObjectIdentifier> keys = new ArrayList<>();
      keys.add(objectId);

      Delete del = Delete.builder().objects(keys).build();

      DeleteObjectsRequest objectDeleteRequest = DeleteObjectsRequest.builder()
          .bucket(bucketName).delete(del).build();
      s3.deleteObjects(objectDeleteRequest);
    } else {
      System.out.println("File does not exist");
    }
  }
}
