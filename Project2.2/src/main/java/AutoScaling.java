import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.*;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.*;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.ec2.model.Instance;

import com.amazonaws.services.elasticloadbalancing.*;
import com.amazonaws.services.elasticloadbalancing.model.*;
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.GetMethod;

import java.lang.Exception;
import java.io.*;
import java.util.*;

/**
 * This class validates an Auto Scaling test.
 *
 * @version 1.0
 * @Author Yilong Chang Andrew ID: yilongc
 * @Since 12/02/2015
 */
public class AutoScaling {
    /**
     * This method creates a security group that allows all inbound TCP requests.
     *
     * @param ec2       Amazon EC2 Client
     * @param groupName name of the Security Group
     * @return ID of the Security Group
     */
    public static String createSecurityGroup(AmazonEC2Client ec2, String groupName) {
        String securityGroupID = null;
        try {
            //Create a Security Group Request
            //Reference: http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/create-security-group.html
            CreateSecurityGroupRequest createSecurityGroupRequest = new CreateSecurityGroupRequest();

            createSecurityGroupRequest.withGroupName(groupName)
                    .withDescription("Project 2.2 Security Group");

            //Create Security Group
            CreateSecurityGroupResult createSecurityGroupResult = ec2.createSecurityGroup(createSecurityGroupRequest);
            securityGroupID = createSecurityGroupResult.getGroupId();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

        //Create and initialize an IpPermission instance.
        IpPermission ipPermission = new IpPermission();

        ipPermission.withIpRanges("0.0.0.0/0")
                .withIpProtocol("tcp")
                .withFromPort(0)
                .withToPort(65535);

        //Create an Authorization Request
        AuthorizeSecurityGroupIngressRequest authorizeSecurityGroupIngressRequest = new AuthorizeSecurityGroupIngressRequest();

        authorizeSecurityGroupIngressRequest.withGroupName(groupName).withIpPermissions(ipPermission);
        //Authorize
        ec2.authorizeSecurityGroupIngress(authorizeSecurityGroupIngressRequest);
        return securityGroupID;
    }

    /**
     * This method creates an elastic load balancer.
     *
     * @param elb             Amazon Elastic LoadBalancing Client
     * @param securityGroupID Security Group ID
     * @return the public DNS of the newly created elastic load balancer
     */
    public static String createElasticLoadBalancer(AmazonElasticLoadBalancingClient elb, String securityGroupID) {
        //Tag
        com.amazonaws.services.elasticloadbalancing.model.Tag elbTag = new com.amazonaws.services.elasticloadbalancing.model.Tag();
        elbTag.withKey("Project").withValue("2.2");

        //Create ELB
        CreateLoadBalancerRequest createloadBalancerRequest = new CreateLoadBalancerRequest();
        createloadBalancerRequest.withLoadBalancerName("ProjectELB")
                .withTags(elbTag)
                .withSecurityGroups(securityGroupID)
                .withAvailabilityZones("us-east-1d")
                .withListeners(new Listener("HTTP", 80, 80));
        //Create
        CreateLoadBalancerResult createLoadBalancerResult = elb.createLoadBalancer(createloadBalancerRequest);
        //Health Check
        HealthCheck healthCheck = new HealthCheck();

        healthCheck.withTarget("HTTP:80/heartbeat")
                .withHealthyThreshold(2)
                .withUnhealthyThreshold(2)
                .withInterval(60)
                .withTimeout(5);
        ConfigureHealthCheckRequest configureHealthCheckRequest = new ConfigureHealthCheckRequest("ProjectELB", healthCheck);
        elb.configureHealthCheck(configureHealthCheckRequest);
        return createLoadBalancerResult.getDNSName();
    }

    /**
     * This method creates an Auto Scaling Group with certain settings.
     *
     * @param aasc            Amazon AutoScaling Client
     * @param securityGroupID Security Group ID
     * @param scaleOutACWC    Amazon CloudWatch Client for scaling out
     * @param scaleInACWC     Amazon CloudWatch Client for scaling in
     */
    public static void createAutoScalingGroup(AmazonAutoScalingClient aasc, String securityGroupID, AmazonCloudWatchClient scaleOutACWC, AmazonCloudWatchClient scaleInACWC) {
        //Tag
        com.amazonaws.services.autoscaling.model.Tag asgTag = new com.amazonaws.services.autoscaling.model.Tag();
        asgTag.withKey("Project").withValue("2.2");
        //Launch configuration
        CreateLaunchConfigurationRequest createLaunchConfigurationRequest = new CreateLaunchConfigurationRequest();
        createLaunchConfigurationRequest.withLaunchConfigurationName("launchConfiguration")
                .withSecurityGroups(securityGroupID)
                .withKeyName("proj1")
                .withImageId("ami-7c0a4614")
                .withInstanceType("m1.small");
        //Create ASG
        CreateAutoScalingGroupRequest createAutoScalingGroupRequest = new CreateAutoScalingGroupRequest();
        createAutoScalingGroupRequest.withAutoScalingGroupName("ProjectASG")
                .withAvailabilityZones("us-east-1d")
                .withMinSize(3)
                .withMaxSize(10)
                .withTags(asgTag)
                .withLoadBalancerNames("ProjectELB")
                .withDefaultCooldown(120)
                .withDesiredCapacity(5)
                .withHealthCheckGracePeriod(120)
                .withHealthCheckType("ELB")
                .withLaunchConfigurationName("launchConfiguration");
        aasc.createLaunchConfiguration(createLaunchConfigurationRequest);
        aasc.createAutoScalingGroup(createAutoScalingGroupRequest);

        /*
        Scaling Policy
         */
        //Scale out policy
        PutScalingPolicyRequest scaleOutPolicyRequest = new PutScalingPolicyRequest();
        scaleOutPolicyRequest.withAutoScalingGroupName("ProjectASG")
                .withPolicyName("Scale out")
                .withScalingAdjustment(3)
                .withAdjustmentType("ChangeInCapacity")
                .withCooldown(120);

        PutScalingPolicyResult scalingOutResult = aasc.putScalingPolicy(scaleOutPolicyRequest);

        //Scale in policy
        PutScalingPolicyRequest scaleInPolicyRequest = new PutScalingPolicyRequest();
        scaleInPolicyRequest.withAutoScalingGroupName("ProjectASG")
                .withPolicyName("Scale in")
                .withScalingAdjustment(-2)
                .withAdjustmentType("ChangeInCapacity")
                .withCooldown(120);
        PutScalingPolicyResult scalingInResult = aasc.putScalingPolicy(scaleInPolicyRequest);
        String outARN = scalingOutResult.getPolicyARN(); // Store ARN
        String inARN = scalingInResult.getPolicyARN();


        //Scale out metric
        Dimension dimension = new Dimension();
        dimension.withName("AutoScalingGroupName").withValue("ProjectASG");

        PutMetricAlarmRequest outMAR = new PutMetricAlarmRequest();
        outMAR.withMetricName("CPUUtilization")
                .withAlarmName("Overload")
                .withNamespace("AWS/EC2")
                .withDimensions(dimension)
                .withUnit(StandardUnit.Percent)
                .withComparisonOperator(ComparisonOperator.GreaterThanOrEqualToThreshold)
                .withStatistic(Statistic.Average)
                .withThreshold(88d)
                .withPeriod(60)
                .withEvaluationPeriods(2)
                .withAlarmActions(outARN);

        //Scale in metric
        PutMetricAlarmRequest inMAR = new PutMetricAlarmRequest();
        inMAR.withMetricName("CPUUtilization")
                .withAlarmName("Underload")
                .withNamespace("AWS/EC2")
                .withDimensions(dimension)
                .withUnit(StandardUnit.Percent)
                .withComparisonOperator(ComparisonOperator.LessThanOrEqualToThreshold)
                .withStatistic(Statistic.Average)
                .withThreshold(68d)
                .withPeriod(60)
                .withEvaluationPeriods(3)
                .withAlarmActions(inARN);
        //Use these alarms
        scaleOutACWC.putMetricAlarm(outMAR);
        scaleInACWC.putMetricAlarm(inMAR);
    }

    /**
     * This method uses an httpclient to send http request to start a warm up.
     *
     * @param loadGeneratorDNS       public DNS of load generator
     * @param elasticloadBalancerDNS public DNS of elastic load balancer
     */
    public static void warmUp(String loadGeneratorDNS, String elasticloadBalancerDNS) {
        String url;
        try {
            //Reference: http://hc.apache.org/httpclient-legacy/tutorial.html
            url = "http://" + loadGeneratorDNS + "/warmup?dns=" + elasticloadBalancerDNS;
            HttpClient httpClient = new HttpClient();
            HttpMethod method = new GetMethod(url);
            //Execute the GetMethod
            httpClient.executeMethod(method);
            System.out.println("Start warm up");
            method.releaseConnection();
            Thread.sleep(320000);
        } catch (HttpException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * This method uses an httpclient to send http request to start the test and get test ID.
     *
     * @param loadGeneratorDNS       public DNS of load generator
     * @param elasticloadBalancerDNS public DNS of load balancer
     * @return test ID
     */
    public static String getTestID(String loadGeneratorDNS, String elasticloadBalancerDNS) {
        String url;
        BufferedReader bufferedReader;
        String line;
        String testID = null;
        try {
            url = "http://" + loadGeneratorDNS + "/junior?dns=" + elasticloadBalancerDNS;
            HttpClient httpClient = new HttpClient();
            HttpMethod method = new GetMethod(url);
            //Execute the GetMethod
            httpClient.executeMethod(method);
            System.out.println("Start test.");
            //Use buffer reader to read the HTTP file and catch the test ID
            bufferedReader = new BufferedReader(new InputStreamReader(method.getResponseBodyAsStream()));
            while ((line = bufferedReader.readLine()) != null) {
                int idStart = line.indexOf("log?name=test.");
                if (idStart != -1) {
                    testID = line.substring(idStart + 14, idStart + 27);
                    System.out.println(testID);
                }
            }
            //Close the Reader
            bufferedReader.close();
            //Release connection
            method.releaseConnection();
        } catch (HttpException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return testID;
    }

    /**
     * This main method creates security group, ASG, load balancer and data centers to conduct an auto scaling test.
     *
     * @param args the command line arguments
     * @throws Exception exception that may occur
     */
    public static void main(String[] args) throws Exception {
        //Load the Properties File with AWS Credentials
        Properties properties = new Properties();
        properties.load(AutoScaling.class.getResourceAsStream("/AwsCredentials.properties"));
        BasicAWSCredentials bawsc = new BasicAWSCredentials(properties.getProperty("accessKey"), properties.getProperty("secretKey"));
        //Create Amazon EC2 Client, Amazon Elastic Load Balancing Client, Auto Scaling Client, Cloud Watch Client
        AmazonEC2Client ec2 = new AmazonEC2Client(bawsc);
        AmazonElasticLoadBalancingClient elb = new AmazonElasticLoadBalancingClient(bawsc);
        AmazonAutoScalingClient aasc = new AmazonAutoScalingClient(bawsc);
        AmazonCloudWatchClient scaleOutACWC = new AmazonCloudWatchClient(bawsc);
        AmazonCloudWatchClient scaleInACWC = new AmazonCloudWatchClient(bawsc);
        //Create Security Group
        String securityGroupID = createSecurityGroup(ec2, "Project2_2");
        //Store the load generator public DNS
        String loadGeneratorDNS = "ec2-52-1-83-242.compute-1.amazonaws.com";
        //Create ELB
        String elbDNS = createElasticLoadBalancer(elb, securityGroupID);
        //Create ASG
        createAutoScalingGroup(aasc, securityGroupID, scaleOutACWC, scaleInACWC);

        /*
        Warm up on load generator
        */
        //Wait til instances ready
        Thread.sleep(300000);
        for (int i = 0; i < 5; i++) {
            System.out.println("Warm up " + i + " ended.");
            warmUp(loadGeneratorDNS, elbDNS);
        }
        //Start test and get Test ID
        String testID = getTestID(loadGeneratorDNS, elbDNS);
        //Wait for the test to complete
        Thread.sleep(3000000);
        /*
        Delete all the created
         */
        //Delete Alarm
        DeleteAlarmsRequest deleteAlarmsRequest = new DeleteAlarmsRequest();
        deleteAlarmsRequest.withAlarmNames("CPUHigh", "CPULow");
        AmazonCloudWatchClient acwc = new AmazonCloudWatchClient(bawsc);
        acwc.deleteAlarms(deleteAlarmsRequest);
        System.out.println("Alarm deleted.");
        Thread.sleep(30000);
        //Delete Policy
        DeletePolicyRequest deletePolicyRequest = new DeletePolicyRequest();
        deletePolicyRequest.withPolicyName("Scale out");
        deletePolicyRequest.withPolicyName("Scale in");
        System.out.println("Policy deleted.");
        Thread.sleep(30000);
        //Update ASG setting
        UpdateAutoScalingGroupRequest updateAutoScalingGroupRequest = new UpdateAutoScalingGroupRequest();
        updateAutoScalingGroupRequest.withAutoScalingGroupName("ProjectASG")
                .withMaxSize(0)
                .withMinSize(0)
                .withDesiredCapacity(0);
        AmazonAutoScalingClient amazonAutoScalingClient = new AmazonAutoScalingClient(bawsc);
        amazonAutoScalingClient.updateAutoScalingGroup(updateAutoScalingGroupRequest);
        System.out.println("ASG setting set.");
        Thread.sleep(120000);
        /*Get and Terminate instances*/
        //Use reservation to get instances by id other than load generator
        List<Reservation> reservations = ec2.describeInstances().getReservations();
        Collection<String> instanceIDs = new ArrayList<String>();
        int reservationCount = reservations.size();
        for (int i = 0; i < reservationCount; i++) {
            List<Instance> instances = reservations.get(i).getInstances();
            int instanceCount = instances.size();
            //Find the instance with the exact instance ID
            for (int j = 0; j < instanceCount; j++) {
                Instance instance = instances.get(j);
                if (!instance.getPublicDnsName().equals(loadGeneratorDNS)) {
                    instanceIDs.add(instance.getInstanceId());
                }
            }
        }
        //Terminate instances by id
        TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest();
        terminateInstancesRequest.withInstanceIds(instanceIDs);
        ec2.terminateInstances(terminateInstancesRequest);
        System.out.println("Instances deleted.");
        Thread.sleep(30000);
        //Delete ASG
        DeleteAutoScalingGroupRequest deleteAutoScalingGroupRequest = new DeleteAutoScalingGroupRequest();
        deleteAutoScalingGroupRequest.withAutoScalingGroupName("ProjectASG");
        aasc.deleteAutoScalingGroup(deleteAutoScalingGroupRequest);
        System.out.println("ASG deleted.");
        Thread.sleep(60000);
        //Delete Launch Configuration
        DeleteLaunchConfigurationRequest deleteLaunchConfigurationRequest = new DeleteLaunchConfigurationRequest();
        deleteLaunchConfigurationRequest.withLaunchConfigurationName("launchConfiguration");
        aasc.deleteLaunchConfiguration(deleteLaunchConfigurationRequest);
        System.out.println("Launch Configuration deleted.");
        Thread.sleep(30000);
        //Delete Load Balancer
        DeleteLoadBalancerRequest deleteLoadBalancerRequest = new DeleteLoadBalancerRequest();
        deleteLoadBalancerRequest.withLoadBalancerName("ProjectELB");
        elb.deleteLoadBalancer(deleteLoadBalancerRequest);
        System.out.println("ELB deleted.");
        Thread.sleep(50000);
        //Delete Security Group
        DeleteSecurityGroupRequest deleteSecurityGroupRequest = new DeleteSecurityGroupRequest();
        deleteSecurityGroupRequest.withGroupId(securityGroupID);
        ec2.deleteSecurityGroup(deleteSecurityGroupRequest);
        System.out.println("Security Group deleted.");
    }
}
