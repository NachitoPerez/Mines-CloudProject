package fr.emse.ec2;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;

public class CreateEC2 {

  public static void main(String[] args) {
    Region region = Region.US_EAST_1;

    if (args.length < 1) {
      System.out.println("Missing the AMI ID argument");
      System.exit(1);
    }

    // Create a EC2 client
    Ec2Client ec2 = Ec2Client.builder().region(region).build();

    // Create a request to create VM instance
    RunInstancesRequest runRequest = RunInstancesRequest.builder()
        .imageId(args[0]).instanceType(InstanceType.T2_MICRO).keyName("vockey")
        .maxCount(1).minCount(1).build();

    // Execute the request
    RunInstancesResponse response = ec2.runInstances(runRequest);

    // Get the Id of the instance created
    String instanceId = response.instances().get(0).instanceId();

    // Create the Tag for the instance
    Tag tag = Tag.builder().key("Name").value("My Instance").build();

    // Create a request to set the Tag
    CreateTagsRequest tagRequest = CreateTagsRequest.builder()
        .resources(instanceId).tags(tag).build();

    try {
      ec2.createTags(tagRequest);

      System.out.println("EC2 " + instanceId + " successfully created");

    } catch (Ec2Exception e) {
      e.printStackTrace();
    }
  }
}
