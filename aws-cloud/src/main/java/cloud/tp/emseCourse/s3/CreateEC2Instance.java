package cloud.tp.emseCourse.s3;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

public class CreateEC2Instance {

    public static void main(String[] args) {
        String amiId = "ami-0e8a34246278c21e4";

        // Create an EC2 client
        Ec2Client ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();

        // Specify the details for the new EC2 US_EAST_1instance
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(1)
                .minCount(1)
                .build();

        // Launch the EC2 instance
        RunInstancesResponse response = ec2.runInstances(runRequest);

        // Get the instance ID
        String instanceId = response.instances().get(0).instanceId();

        System.out.println("EC2 instance created with ID: " + instanceId);

        // Terminate the EC2 instance (optional)
        // Uncomment the following lines if you want to terminate the instance immediately
        /*
        TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();
        ec2.terminateInstances(terminateRequest);
        */

        // Close the EC2 client
        ec2.close();
    }
}
