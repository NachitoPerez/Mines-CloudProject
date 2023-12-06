package fr.emse.sqs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class RetrieveMessage {

	public static void main(String[] args) {
		Region region = Region.US_EAST_1;

		if (args.length < 1) {
			System.out.println("Missing the Queue URL argument");
			System.exit(1);
		}

		String queueURL = args[0];

		SqsClient sqsClient = SqsClient.builder().region(region).build();

		ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder().queueUrl(queueURL).maxNumberOfMessages(1)
				.build();

		List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

		if (!messages.isEmpty()) {
			Message msg = messages.get(0);

			String[] arguments = msg.body().split(";");
			String bucketName = arguments[0];
			String fileName = arguments[1];

			S3Client s3 = S3Client.builder().region(region).build();

			// Check file exists
			ListObjectsRequest listObjects = ListObjectsRequest.builder().bucket(bucketName).build();

			ListObjectsResponse res = s3.listObjects(listObjects);
			List<S3Object> objects = res.contents();

			if (objects.stream().anyMatch((S3Object x) -> x.key().equals(fileName))) {

				// Retrieve file
				GetObjectRequest objectRequest = GetObjectRequest.builder().key(fileName).bucket(bucketName).build();

				ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
				byte[] data = objectBytes.asByteArray();

				File file = new File(fileName);
				try (OutputStream os = new FileOutputStream(file)) {
					os.write(data);
				} catch (IOException e) {
					e.printStackTrace();
				}

				// Delete the message
				for (Message message : messages) {
					DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(queueURL)
							.receiptHandle(message.receiptHandle()).build();

					sqsClient.deleteMessage(deleteMessageRequest);
				}
			} else {
				System.out.println("File is not available in the Bucket");
			}
		} else {
			System.out.println("Queue is empty");
		}
	}
}
