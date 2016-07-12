/*
 * MSB.java
 *
 * This is a web-service used by the MSB to get targets' private
 * conversations from the databases. The conversations have been
 * encrypted, but I have heard rumors about the key being a part
 * of the results retrieved from the database.
 *
 * 02/08/15 - I have replicated the database instances to make
 * the web service go faster.
 *
 * To do (before 02/15/15): My team lead says that I can get a
 * higher RPS by optimizing the retrieveDetails function. I
 * stack overflowed "how to optimize retrieveDetails function",
 * but could not find any helpful results. I need to get it done
 * before 02/15/15 or I will lose my job to that new junior systems
 * architect.
 *
 * 02/15/15 - :'(
 *
 *
 */

import org.vertx.java.core.Handler;

import org.vertx.java.core.http.HttpServerRequest;

import org.vertx.java.platform.Verticle;


import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;


public class MSB extends Verticle {
    private static final int MAX_ENTRIES = 400;
    private static final int MAX_ENTRIESL3 = 200;
    //cache 1, size 400, for ascending pattern
    private LinkedHashMap<String, String> cacheL1 = new LinkedHashMap<String, String>(1 << 9) {
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            return size() > MAX_ENTRIES;
        }
    };
    //cache 2, size 400, for descending pattern
    private LinkedHashMap<String, String> cacheL2 = new LinkedHashMap<String, String>(1 << 9) {
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            return size() > MAX_ENTRIES;
        }
    };
    //cache L3 for other patterns.
    private LinkedHashMap<String, String> cacheL3 = new LinkedHashMap<String, String>(1 << 8) {
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            return size() > MAX_ENTRIESL3;
        }
    };

    private String[] databaseInstances = new String[2];
    // Store last request ID
    //private Integer lastID = new Integer(1);
    //private int pattern1 = 0, pattern2 = 0;

    /*
     * init -initializes the variables which store the
     *	     DNS of your database instances
     */
    private void init() {
        /* Add the DNS of your database instances here */
        databaseInstances[0] = "ec2-54-152-87-80.compute-1.amazonaws.com";
        databaseInstances[1] = "ec2-54-152-18-224.compute-1.amazonaws.com";
        //Store in advance
        try {
            storeInCache(sendRequest(generateRangeURL(0, 1, 100)), cacheL3);
            storeInCache(sendRequest(generateRangeURL(0, 101, 500)), cacheL1);
            storeInCache(sendRequest(generateRangeURL(1, 853283, 853682)), cacheL2);
            for (int i = 1; i < 86; i++) {
                storeInCache(sendRequest(generateURL(1, Integer.toString(i * 10000))), cacheL3);
            }
            for (int i = 0; i < 19; i++) {
                sendRequest(generateRangeURL(0, i * 500 + 501, i * 500 + 1000));
            }
            for (int i = 0; i < 19; i++) {
                sendRequest(generateRangeURL(1, 840001 + i * 500, 840000 + i * 500 + 500));
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    /**
     * This method stores conversations retrieved from data centers into caches
     *
     * @param lines conversations
     * @param cache cache
     */
    private void storeInCache(String lines, HashMap cache) {
        String[] tokens = lines.split(";");
        for (int i = 0; i < tokens.length; i++) {
            cache.put(tokens[i].split(" ")[1], tokens[i]);
        }
    }

    /*
     * checkBackend - verifies that the DCI are running before starting this server
     */
    private boolean checkBackend() {
        try {
            if (sendRequest(generateURL(0, "1")) == null ||
                    sendRequest(generateURL(1, "1")) == null)
                return true;
        } catch (Exception ex) {
            System.out.println("Exception is " + ex);
            return true;
        }
        return false;
    }

    /*
     * sendRequest
     * Input: URL
     * Action: Send a HTTP GET request for that URL and get the response
     * Returns: The response
     */
    private String sendRequest(String requestUrl) throws Exception {

        URL url = new URL(requestUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        connection.setRequestMethod("GET");
        connection.setRequestProperty("User-Agent", "Mozilla/5.0");

        BufferedReader in = new BufferedReader(
                new InputStreamReader(connection.getInputStream(), "UTF-8"));

        String responseCode = Integer.toString(connection.getResponseCode());
        if (responseCode.startsWith("2")) {
            String inputLine;
            StringBuffer response = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            connection.disconnect();
            return response.toString();
        } else {
            System.out.println("Unable to connect to " + requestUrl +
                    ". Please check whether the instance is up and also the security group settings");
            connection.disconnect();
            return null;
        }
    }

    /*
     * generateURL
     * Input: Instance ID of the Data Center
     * 		  targetID
     * Returns: URL which can be used to retrieve the target's details
     * 			from the data center instance
     * Additional info: the target's details are cached on backend instance
     */
    private String generateURL(Integer instanceID, String key) {
        return "http://" + databaseInstances[instanceID] + "/target?targetID=" + key;
    }

    /*
     * generateRangeURL
     * Input: 	Instance ID of the Data Center
     * 		  	startRange - starting range (targetID)
     *			endRange - ending range (targetID)
     * Returns: URL which can be used to retrieve the details of all
     * 			targets in the range from the data center instance
     * Additional info: the details of the last 10,000 targets are cached
     * 					in the database instance
     *
     */
    private String generateRangeURL(Integer instanceID, Integer startRange, Integer endRange) {
        return "http://" + databaseInstances[instanceID] + "/range?start_range="
                + Integer.toString(startRange) + "&end_range=" + Integer.toString(endRange);
    }

    /*
     * retrieveDetails - you have to modify this function to achieve a higher RPS value
     * Input: the targetID
     * Returns: The result from querying the database instance
     */
    private String retrieveDetails(String targetID) {
        try {
            //First check if in caches
            if (cacheL1.containsKey(targetID)) {
                System.out.println("Find ID: " + targetID);
                return cacheL1.get(targetID);
            } else if (cacheL2.containsKey(targetID)) {
                System.out.println("Find ID: " + targetID);
                return cacheL2.get(targetID);
            } else if (cacheL3.containsKey(targetID)) {
                System.out.println("Find ID: " + targetID);
                return cacheL3.get(targetID);
            } else {
                //If not in caches, check ID number and retrieve from data centers
                if (Integer.parseInt(targetID) < 420000) {
                    storeInCache(sendRequest(generateRangeURL(0, Integer.parseInt(targetID), Integer.parseInt(targetID) + 79)), cacheL1);
                } else {
                    storeInCache(sendRequest(generateRangeURL(1, Integer.parseInt(targetID) - 79, Integer.parseInt(targetID))), cacheL2);
                }
            }
            //If no pattern, store this time's target and conversation in cache, and retrieve
            //lastID = Integer.valueOf(targetID);
            return retrieveDetails(targetID);
        } catch (Exception ex) {
            System.out.println(ex);
            return null;
        }
    }

    /*
     * processRequest - calls the retrieveDetails function with the targetID
     */

    private void processRequest(String targetID, HttpServerRequest req) {
        String result = retrieveDetails(targetID);
        if (result != null)
            req.response().end(result);
        else
            req.response().end("No response received");
    }

    /*
     * start - starts the server
     */
    public void start() {
        init();
        if (!checkBackend()) {
            vertx.createHttpServer().requestHandler(new Handler<HttpServerRequest>() {
                public void handle(HttpServerRequest req) {
                    String query_type = req.path();
                    req.response().headers().set("Content-Type", "text/plain");

                    if (query_type.equals("/target")) {
                        String key = req.params().get("targetID");
                        processRequest(key, req);
                    } else {
                        String key = "1";
                        processRequest(key, req);
                    }
                }
            }).listen(80);
        } else {
            System.out.println("Please make sure that both your DCI are up and running");
            System.exit(0);
        }
    }
}
