package cloud.tp.emseCourse.tds.sqs;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class SendMessage {

	public static void main(String[] args) {
		Region region = Region.US_EAST_1;

		if (args.length < 3) {
			System.out.println("Missing the Queue URL, Bucket Name, or File Name arguments");
			System.exit(1);
		}

		String queueURL = args[0];
		String bucketName = args[1];
		String fileName = args[2];

		SqsClient sqsClient = SqsClient.builder().region(region).build();

		SendMessageRequest sendRequest = SendMessageRequest.builder().queueUrl(queueURL)
				.messageBody(bucketName + ";" + fileName).build();

		SendMessageResponse sqsResponse = sqsClient.sendMessage(sendRequest);

		System.out.println(
				sqsResponse.messageId() + " Message sent. Status is " + sqsResponse.sdkHttpResponse().statusCode());
	}
}
