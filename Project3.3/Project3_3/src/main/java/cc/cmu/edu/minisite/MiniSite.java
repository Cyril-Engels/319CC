package cc.cmu.edu.minisite;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.*;
import java.lang.Integer;

import io.undertow.io.Sender;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.io.IoCallback;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

// hbase api import
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

// dynamodb api import
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;


import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Map;
import java.util.HashMap;


public class MiniSite {

    public MiniSite() throws Exception {

    }

    public static void main(String[] args) throws Exception {
        final MiniSite minisite = new MiniSite();
        final ObjectMapper mapper = new ObjectMapper();
        final DBConnection dbConnection = new DBConnection();
        final HBaseConnection hBaseConnection = new HBaseConnection();
        final DynamoDBConnection dynamoDBConnection = new DynamoDBConnection();

        Undertow.builder()
                .addHttpListener(8080, "0.0.0.0")
                .setHandler(new HttpHandler() {

                    public void handleRequest(final HttpServerExchange exchange) throws Exception {
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json; encoding=UTF-8");
                        Sender sender = exchange.getResponseSender();
                        if (exchange.getRequestPath().equals("/step1")) {//Step 1
                            JSONObject response = new JSONObject();
                            //Query
                            String responseValue = dbConnection.login(exchange.getQueryParameters().get("id").peek(), exchange.getQueryParameters().get("pwd").peek());
                            response.put("name", responseValue);
                            String content = "returnRes(" + mapper.writeValueAsString(response) + ")";
                            sender.send(content);
                        } else if (exchange.getRequestPath().equals("/step2")) {//Step 2
                            JSONObject response = new JSONObject();
                            JSONArray friends = new JSONArray();
                            String userid = exchange.getQueryParameters().get("id").peek();
                            //Query
                            String[] responseValue = hBaseConnection.query(userid).split(":");
                            //Split the friends String
                            for (int i = 0; i < responseValue.length; i++) {
                                JSONObject friend = new JSONObject();
                                friend.put("userid", responseValue[i]);
                                friends.add(friend);
                            }
                            response.put("friends", friends);
                            String content = "returnRes(" + mapper.writeValueAsString(response) + ")";
                            sender.send(content);
                        } else if (exchange.getRequestPath().equals("/step3")) {//Step 3
                            JSONObject response = new JSONObject();
                            String userid = exchange.getQueryParameters().get("id").peek();
                            //Query
                            String[] responseValue = dynamoDBConnection.getImage(userid).split("\t");
                            if (responseValue.length != 0) {
                                response.put("time", responseValue[0]);
                                response.put("url", responseValue[1]);
                            }
                            String content = "returnRes(" + mapper.writeValueAsString(response) + ")";
                            sender.send(content);
                        } else if (exchange.getRequestPath().equals("/step4")) {//Step 4
                            JSONObject response = new JSONObject();
                            JSONArray photos = new JSONArray();
                            String userid = exchange.getQueryParameters().get("id").peek();
                            //Check user id and password
                            String userName = dbConnection.login(userid, exchange.getQueryParameters().get("pwd").peek());
                            //If a pair
                            if (userName != "Unauthorized") {
                                //Search all friends and append name
                                String[] friendsID = hBaseConnection.query(userid).split(":");
                                ArrayList<String> friendsImages = new ArrayList<String>();
                                for (int i = 0; i < friendsID.length; i++) {
                                    String friendName = dbConnection.userName(friendsID[i]);
                                    String[] friendImageInfo = dynamoDBConnection.getImage(friendsID[i]).split("\t");
                                    String friendImage = friendImageInfo[0] + "\t" + friendName + "\t" + friendImageInfo[1];
                                    friendsImages.add(friendImage);
                                }
                                //Sort
                                Collections.sort(friendsImages);
                                /* Turn to json*/
                                for (int i = 0; i < friendsImages.size(); i++) {
                                    String[] friendImageInfo = friendsImages.get(i).split("\t");
                                    JSONObject photo = new JSONObject();
                                    photo.put("url", friendImageInfo[2]);
                                    photo.put("time", friendImageInfo[0]);
                                    photo.put("name", friendImageInfo[1]);
                                    photos.add(photo);
                                }
                            }
                            response.put("name", userName);
                            response.put("photos", photos);
                            String content = "returnRes(" + mapper.writeValueAsString(response) + ")";
                            sender.send(content);
                        }
                    }
                }).build().start();
    }
}

/**
 * This class is the MySQL connection class.
 *
 * @author Yilong Chang
 * @since 20/03/2015
 */
class DBConnection {

    private Connection connection;
    private PreparedStatement preparedStatement;
    private ResultSet resultSet;

    public DBConnection() {
        this.connection = getConnection();
        //System.out.println("Connection Success");
    }

    /**
     * Connect to MySQL
     *
     * @return connection
     */
    public static Connection getConnection() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://project3.cztgquzmwgek.us-east-1.rds.amazonaws.com:3306/project3";
            String username = "root";
            String password = "15619project";

            Connection con = DriverManager.getConnection(url, username, password);
            return con;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    /**
     * Check if userid and password are a pair.
     *
     * @param userid use rid
     * @param pwd    password
     * @return username if a pair, "Unauthorize" otherwise
     */
    public String login(String userid, String pwd) {
        String name = null;
        try {
            //Query
            preparedStatement = connection.prepareStatement("SELECT username FROM userinfo WHERE userid = ? AND password = ?");
            preparedStatement.setString(1, userid);
            preparedStatement.setString(2, pwd);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                name = resultSet.getString("username");
            }
            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return name == null ? "Unauthorized" : name;
    }

    /**
     * Return user name of the given userid
     *
     * @param userid user id
     * @return user name
     */
    public String userName(String userid) {
        String name = null;
        try {//Query
            preparedStatement = connection.prepareStatement("SELECT username FROM userinfo WHERE userid = ?");
            preparedStatement.setString(1, userid);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                name = resultSet.getString("username");
            }
            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return name;
    }
}

/**
 * This class is the HBase connection class.
 *
 * @author Yilong Chang
 * @since 21/03/2015
 */
class HBaseConnection {

    public static Configuration configuration = null;
    public final static byte[] TABLE_NAME = Bytes.toBytes("relationship");
    public final static byte[] FAMILY_NAME = Bytes.toBytes("followed");
    public final static byte[] COLUMN_NAME = Bytes.toBytes("users");
    public static HConnection connection;

    static {
        //Configure
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "172.31.62.112");
        configuration.set("hbase.master", "172.31.62.112:60000");
        try {
            connection = HConnectionManager.createConnection(configuration);
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
        //configuration.addResource(new Path("/usr/local/0.94.26/conf/hbase-site.xml"));
    }

    /**
     * Query by rowkey
     *
     * @param rowKey rowkey of the request
     * @return friends IDs of the given user id
     */
    public String query(String rowKey) {

        HTableInterface table;
        try {
            table = connection.getTable(TABLE_NAME);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            String values = Bytes.toString(result.getValue(FAMILY_NAME, COLUMN_NAME));
            table.close();
            return values;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}

/**
 * This class is the DynamoDB connection class.
 *
 * @author Yilong Chang
 * @since 21/03/2015
 */
class DynamoDBConnection {
    //
    private static final String TABLE_NAME = "userimage";
    static AmazonDynamoDBClient dynamoDB = new AmazonDynamoDBClient(new ProfileCredentialsProvider().getCredentials());

    public String getImage(String userid) {
        //Table table = dynamoDB.getTable(TABLE_NAME);

        AttributeValue attributeUser = new AttributeValue().withN(userid);

        GetItemRequest getItemRequest = new GetItemRequest().withTableName("userimage");
        getItemRequest.addKeyEntry("userid", attributeUser);
        GetItemResult getItemResult = dynamoDB.getItem(getItemRequest);
        Map<String, AttributeValue> item = getItemResult.getItem();
        //Iterator<AttributeValue> iterator = item.values().iterator();
        String created_time = item.get("time").getS();
        String image_url = item.get("url").getS();
        return  created_time + "\t" + image_url;
    }
}
