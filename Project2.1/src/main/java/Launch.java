import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;

import java.lang.Exception;
import java.io.*;
import java.util.*;
import java.net.*;

import org.ini4j.*;
import org.ini4j.Profile.Section;

/**
 * This class sets up load generator and data centers and implements a horizontal test.
 *
 * @author Yilong Chang Addrew ID: yilongc
 * @version 1.0
 * @since 05/02/2015
 */
public class Launch {
    /**
     * This method validates the creating of a Security Group.
     *
     * @param ec2 Amazon EC2 Client
     * @param groupName name of security group
     */
    public static void createSecurityGroup(AmazonEC2Client ec2, String groupName) {
        try {
            //Create a Security Group Request
            //Reference: http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/create-security-group.html
            CreateSecurityGroupRequest createSecurityGroupRequest = new CreateSecurityGroupRequest();

            createSecurityGroupRequest.withGroupName(groupName)
                    .withDescription("Project 2.1 Security Group");

            //Create Security Group
            CreateSecurityGroupResult createSecurityGroupResult = ec2.createSecurityGroup(createSecurityGroupRequest);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

        //Create and initialize an IpPermission instance.
        IpPermission ipPermission = new IpPermission();

        ipPermission.withIpRanges("0.0.0.0/0")
                .withIpProtocol("tcp")
                .withFromPort(80)
                .withToPort(80);
        //Create an Authorization Request
        AuthorizeSecurityGroupIngressRequest authorizeSecurityGroupIngressRequest = new AuthorizeSecurityGroupIngressRequest();

        authorizeSecurityGroupIngressRequest.withGroupName("Project2_1_Java").withIpPermissions(ipPermission);
        //Authorize
        ec2.authorizeSecurityGroupIngress(authorizeSecurityGroupIngressRequest);
    }

    /**
     * This method validates the creating of an instance.
     * @param ec2 Amazon EC2 Client
     * @param imageID instance image ID
     * @param instanceType instance type
     * @return newly created instance
     */
    public static Instance createInstance(AmazonEC2Client ec2, String imageID, String instanceType) throws Exception {
        //Create Instance Request
        RunInstancesRequest runInstancesRequest = new RunInstancesRequest();

        //Configure Instance Request
        runInstancesRequest.withImageId(imageID)
                .withInstanceType(instanceType)
                .withMinCount(1)
                .withMaxCount(1)
                .withKeyName("proj1")
                .withSecurityGroups("Project2_1_Java");

        //Launch Instance
        RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);

        //Get the ID of the Instance just launched
        Instance instance = runInstancesResult.getReservation().getInstances().get(0);
        //Add a tag to the Instance
        CreateTagsRequest createTagsRequest = new CreateTagsRequest();
        createTagsRequest.withResources(instance.getInstanceId()).withTags(new Tag("Project", "2.1"));
        ec2.createTags(createTagsRequest);
        // Print the ID of the instance
        System.out.println("Launched an Instance with ID: " + instance.getInstanceId());
        return instance;
    }

    /**
     * This method returns an instance that matches the instance ID.
     *
     * @param instanceID Amazon EC2 Client
     * @return the instance matches the ID
     * @throws Exception Exception that may occur
     */
    public static Instance reservation(AmazonEC2Client amazonEC2Client, String instanceID) throws Exception {
        //Obtain a list of Reservations
        List<Reservation> reservations = amazonEC2Client.describeInstances().getReservations();
        Instance instanceToReturn = new Instance();
        int reservationCount = reservations.size();
        for (int i = 0; i < reservationCount; i++) {
            List<Instance> instances = reservations.get(i).getInstances();
            int instanceCount = instances.size();

            //Find the instance with the exact instance ID
            for (int j = 0; j < instanceCount; j++) {
                Instance instance = instances.get(j);
                if (instance.getInstanceId().equals(instanceID)) {
                    instanceToReturn = instance;
                }
            }
        }
        return instanceToReturn;
    }

    /**
     * This method generates HTTP request to start the horizontal test and uses http GET to return the Test ID.
     * Reference: http://stackoverflow.com/questions/1485708/how-do-i-do-a-http-get-in-java
     * @param loadGeneratorDNS Public DNS of the load generator
     * @param dataCenterDNS Public DNS of the data center
     * @return Test ID of the horizontal test
     */
    public static String getTestID(String loadGeneratorDNS, String dataCenterDNS) {
        URL url;
        HttpURLConnection conn;
        BufferedReader bufferedReader;
        String line;
        String testID = null;
        try {
            url = new URL("http://" + loadGeneratorDNS + "/test/horizontal?dns=" + dataCenterDNS);
            System.out.println(url);
            /* Connect to the URL */
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            //Use buffer reader to read the HTTP file and catch the test ID
            bufferedReader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            while ((line = bufferedReader.readLine()) != null) {
                int idStart = line.indexOf("log?name=test.");
                if (idStart != -1) {
                    testID = line.substring(idStart + 14, idStart + 27);
                }
            }
            //Close the Reader
            bufferedReader.close();
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        return testID;
    }

    /**
     * This method validates adding a new data center to an existing test.
     * @param loadGeneratorDNS Public DNS of the load generator
     * @param dataCenterDNS Public DNS of the new data center
     */
    public static void addDataCenter(String loadGeneratorDNS, String dataCenterDNS) {
        URL url;
        HttpURLConnection conn;
        BufferedReader bufferedReader;
        String line;
        try {
            /* Connect to the URL and add a new data center to test via reading*/
            url = new URL("http://" + loadGeneratorDNS + "/test/horizontal/add?dns=" + dataCenterDNS);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            bufferedReader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    /**
     * This main method creates a load generator and a data center to start a horizontal test,
     * and keeps adding new data centers one at a time if the RPS is less than 4000.
     *
     * @param args command line arguments
     * @throws Exception Exceptions that may occur
     */
    public static void main(String[] args) throws Exception {

        //Load the Properties File with AWS Credentials
        Properties properties = new Properties();
        properties.load(Launch.class.getResourceAsStream("/AwsCredentials.properties"));
        BasicAWSCredentials bawsc = new BasicAWSCredentials(properties.getProperty("accessKey"), properties.getProperty("secretKey"));
        //Create an Amazon EC2 Client
        AmazonEC2Client ec2 = new AmazonEC2Client(bawsc);
        //createSecurityGroup
        try {
            createSecurityGroup(ec2, "Project2_1_Java");
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

        //Create a LoadGenerator
        Instance loadGenerator = createInstance(ec2, "ami-4c4e0f24", "m3.medium");
        String lGID = loadGenerator.getInstanceId();
        //Wait till instance ready
        Thread.sleep(210000);
        //Update the state of the load generator
        loadGenerator = reservation(ec2, lGID);
        //Get the public DNS of the load generator
        String loadGeneratorDNS = loadGenerator.getPublicDnsName();
        System.out.println(loadGeneratorDNS);

        //Create the first DataCenter
        Instance dataCenter = createInstance(ec2, "ami-b04106d8", "m3.medium");
        String dCID=dataCenter.getInstanceId();
        //Wait till instance ready
        Thread.sleep(210000);
        //Update the state of the data center
        dataCenter=reservation(ec2,dCID);
        //Get the public DNS of the data center
        String dataCenterDNS = dataCenter.getPublicDnsName();
        System.out.println(dataCenterDNS);

        //Start Horizontal Test and get Test ID
        String testID = getTestID(loadGeneratorDNS, dataCenterDNS);
        System.out.println(testID);
        /*Use the Test ID to check if the RPS is less than 4000. If less than 4000, add a new data center and check again */
        URL url;
        /**
         * This variable indicates whether to add a new data center
         */
        boolean needMoreDataCenter = false;
        while (!needMoreDataCenter) {
            //Use Ini parser to parse log
            //Reference: http://stackoverflow.com/questions/1602270/ini4j-how-to-get-all-the-key-names-in-a-setting
            try {
                Thread.sleep(120000);
                url = new URL("http://" + loadGeneratorDNS + "/log?name=test." + testID + ".log");
                System.out.println(url);
                Ini testLog = new Ini(url);
                //Use section name to fetch the RPS
                for (String sectionName : testLog.keySet()) {
                    if (sectionName.startsWith("Minute")) {
                        double RPS = 0;
                        Section section = testLog.get(sectionName);
                        for (String optionKey : section.keySet()) {
                            RPS += Double.parseDouble(section.get(optionKey));
                        }
                        //If RPS less than 4000, set the indicator to true
                        System.out.println(RPS);
                        if (RPS < 4000) {
                            needMoreDataCenter = true;
                        } else {
                            System.out.println("RPS larger than 4000");
                            break;
                        }
                    }
                }
            } catch (IOException e) {
                System.err.println(e.getMessage());
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
            if (needMoreDataCenter) {
                //If less than 4000, create more Data Centers
                Instance newDataCenter = createInstance(ec2, "ami-b04106d8", "m3.medium");
                String newDCID= newDataCenter.getInstanceId();
                //Wait till instance ready
                Thread.sleep(210000);
                //Update the state of the new instance
                newDataCenter=reservation(ec2,newDCID);
                String newDataCenterDNS = newDataCenter.getPublicDnsName();
                addDataCenter(loadGeneratorDNS, newDataCenterDNS);
                //Set the indicator to false and jump into the loop again.
                needMoreDataCenter = false;
            }
        }
    }
}


