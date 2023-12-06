package fr.emse.s3;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

public class StartStopEC2Instance {

    public static void main(String[] args) {    

        String instanceId = "i-040fa4313c68a7864";

        // Create an EC2 client
        Ec2Client ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();

        // Describe the current state of the EC2 instance
        DescribeInstancesRequest describeRequest = DescribeInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        DescribeInstancesResponse describeResponse = ec2.describeInstances(describeRequest);

        Instance instance = describeResponse.reservations().get(0).instances().get(0);
        InstanceState instanceState = instance.state();

        // Check the current state of the EC2 instance
        if (instanceState.name() == InstanceStateName.RUNNING) {
            // Stop the EC2 instance
            StopInstancesRequest stopRequest = StopInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();
            ec2.stopInstances(stopRequest);
            System.out.println("EC2 instance is now stopping.");
        } else {
            // Start the EC2 instance
            StartInstancesRequest startRequest = StartInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();
            ec2.startInstances(startRequest);
            System.out.println("EC2 instance is now starting.");
        }

        // Close the EC2 client
        ec2.close();
    }
}

