package fr.emse.ec2;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstanceStatusRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstanceStatusResponse;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.StopInstancesRequest;

public class StartStopEC2 {

  public static void main(String[] args) {
    Region region = Region.US_EAST_1;

    if (args.length < 1) {
      System.out.println("Missing the Instance Id argument");
      System.exit(1);
    }

    Ec2Client ec2 = Ec2Client.builder().region(region).build();

    DescribeInstanceStatusRequest describeInstanceStatusRequest = DescribeInstanceStatusRequest
        .builder().instanceIds(args[0]).filters(Filter.builder()
            .name("instance-state-name").values("running").build())
        .build();

    DescribeInstanceStatusResponse describeInstanceStatus = ec2
        .describeInstanceStatus(describeInstanceStatusRequest);

    if (describeInstanceStatus.instanceStatuses().size() == 0) {
      StartInstancesRequest startRequest = StartInstancesRequest.builder()
          .instanceIds(args[0]).build();
      ec2.startInstances(startRequest);
    } else {
      StopInstancesRequest stopRequest = StopInstancesRequest.builder()
          .instanceIds(args[0]).build();
      ec2.stopInstances(stopRequest);
    }
  }
}
