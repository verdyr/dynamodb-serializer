/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.s4.serializer.dynamodb;

import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// SB --->>>
import java.io.File;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import com.google.gson.Gson;

import com.google.common.base.Splitter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
//import java.util.Random;

// AWS SDK

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.ComparisonOperator;
import com.amazonaws.services.dynamodb.model.Condition;
import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.PutItemResult;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;
import com.amazonaws.services.dynamodb.model.TableDescription;
import com.amazonaws.services.dynamodb.model.TableStatus;

// SQL Xerial JDBC
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

// <<<--- SB

// keyed and cued by topic name
public class EventCountAndReportPE extends ProcessingElement {

    transient Stream<TopicEvent> downStream;
    transient int threshold = 1; // SB!
    int count;
//    int countUsedEvents = 0; // SB
    boolean firstEvent = true;
    boolean firstInsert = false; // SB

    static Logger logger = LoggerFactory.getLogger(EventCountAndReportPE.class);

// SB --->>>
// Data Quality (0-100%)
    double accuracy = 0.0;
    double accuracyRT = 0.0;
    double confidence = 0.0;
    double relevancy = 0.0;
    double consistency = 0.0;


    int countUsedMsgs = 0;
    static int countReceivedMsgs;
    static int countReceivedMsgsPrev;

    int countUsedMsgs_CV = 0;
    static int countReceivedMsgs_CV;
    static int countReceivedMsgsPrev_CV;

    int countUsedMsgs_NLP = 0;
    static int countReceivedMsgs_NLP;
    static int countReceivedMsgsPrev_NLP;

    int countUsedMsgs_Audio = 0;
    static int countReceivedMsgs_Audio;
    static int countReceivedMsgsPrev_Audio;

    String receivedMsgs;

    static AmazonDynamoDBClient dynamoDBClient;
    static String rtEventsTableName = "Table";

    static String tableDataQuality = "TableQuality";

// DB files, original and extended
// !!!!!!!!!!! CHECK THIS BEFORE AWS DEPLOYMENT !!!!!!!!!!!!
    String db_base_dir ="/media/ephemeral0";

//    public db_orig = enum {db_base_dir + "/cv.db", db_base_dir + "/nlp.db", db_base_dir + "/audio.db"};

    String db_orig = db_base_dir + "/cv.db";

    String query = "cv";

// Connection, Statement, ResultSet declaration
    Connection db_conn = null;
    PreparedStatement db_stmt = null;
    ResultSet rs = null;

// Radius for bounding box can be customised using DataFusion.properties file
// Estimated precision
// 0.000X = 10-20m
// 0.00X  = 50m
// 0.0X   = 100-500m

// Initializing appID and userID
    int userID = 0;
    int appID = 0;

// Timestamp same as in Twitter message, e.g. (EEE MMM d HH:mm:ss Z yyyy)
//    static SimpleDateFormat dateFormatter = new SimpleDateFormat("EEE MMM d HH:mm:ss Z yyyy");
// Timestamp as follows (yyyy MM dd HH:mm)
    static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy MM dd HH:mm");

// AWS creentials structure
    Properties awsProperties = new Properties();
// Structure for Data fusion parameters
    Properties fusionProperties = new Properties();

// <<<---SB

    public EventCountAndReportPE() {
        // required for checkpointing in S4 0.5. Requirement to be removed in 0.6
    }

    public EventCountAndReportPE(App app) {
        super(app);
    }

    public void setDownstream(Stream<TopicEvent> aggregatedTopicStream) {
        this.downStream = aggregatedTopicStream;
    }

    public void onEvent(TopicEvent event) {
        if (firstEvent) {
            logger.info("Handling new Event [{}]", getId());
            firstEvent = false;
	    firstInsert = true;
        }
        count += event.getCount();
//        countUsedEvents++; // SB
//        logger.info("Used Data Events counter [{}]", countUsedEvents); // SB




if (false) { // BEGINNING OF THE BLOCK!!!!!!!!!!!

        if (firstInsert) {

                firstInsert = false;

        try {

// Data fusion config file:
                try {
        //              File fusionPropsFile = new File(System.getProperty("user.home") + "/DataFusion.properties");
                        File fusionPropsFile = new File("/home/ec2-user/DataFusion.properties");
                        if (!fusionPropsFile.exists()) {

                                fusionPropsFile = new File(System.getProperty("user.home") + "/DataFusion.properties");
                                if (!fusionPropsFile.exists()) {
                                        logger.error(
                                        "Cannot find Data fusion properties file in this location :[{}]. Make sure it is available at this place and includes AWS credentials (accessKey, secretKey)",
                                                fusionPropsFile.getAbsolutePath());
                                }
                        }
                        fusionProperties.load(new FileInputStream(fusionPropsFile));
                        accuracy = Double.parseDouble(fusionProperties.getProperty("accuracy"));
                        confidence = Double.parseDouble(fusionProperties.getProperty("confidence"));

                } catch (Exception e) {
                        logger.error("Cannot find Data fusion config file", e);
                }

// Create and configure DynamoDB client
                AWSCredentials credentials = new BasicAWSCredentials(awsProperties.getProperty("accessKey"), awsProperties.getProperty("secretKey"));

                AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(credentials);
                logger.info("Create DynamoDB client");
                dynamoDBClient.setEndpoint("dynamodb.eu-west-1.amazonaws.com");
                logger.info("DynamoDB client credentials are accepted and endpoint selected");


//                try {

                        // Extracted context, e.g query, activity
                        String searchQueryAPI = "Test KnowledgeDiscovery API Query";
                        String object = "Object detected";


                        Map<String, AttributeValue> itemRT = new HashMap<String, AttributeValue>();
                        Map<String, AttributeValue> itemDQ = new HashMap<String, AttributeValue>();

                        Iterable<String> dataSplit = Splitter.on(' ').omitEmptyStrings().trimResults().split(getId());
                        // List<String> dataList = Lists.newArrayList(Elements.getElements(dataSplit));
                        // String receivedMsgs = dataList.get(dataList.size()-1);
                        // countReceivedMsgs = Integer.parseInt(receivedMsgs);;

                        int i = 0;
                        for (String token : dataSplit) {
                                i++;
                                receivedMsgs = token;
                        }
                        int k = 0;
                        for (String token : dataSplit) {
                                k++;
                                if (k == (i - 2)) {
                                        receivedAppID = token;
                                }
                                else if (k == (i - 1)) {
                                        receivedUserID = token;
                                }
                        }

                        appID = Double.parseDouble(receivedAppID);
                        userID = Double.parseDouble(receivedUserID);

// STUPID HARDCODE but fast for prototype, should change to class later :)
                        if (appID == 0 && userID > 0) {
                                // CV app and serialization table
                                rtEventsTableName = "TableEventVector_CV";
                                tableDataQuality = "EventVectorQuality_CV";
                                db_orig = db_base_dir + "/cv.db";
                                countReceivedMsgs_CV = Integer.parseInt(receivedMsgs) - countReceivedMsgsPrev_CV;
                                countReceivedMsgsPrev_CV = Integer.parseInt(receivedMsgs);
                                countUsedMsgs_CV++;
                                countReceivedMsgs = countReceivedMsgs_CV;
                                countUsedMsgs = countUsedMsgs_CV;
                        }
                        else if (appID == 1 && userID > 0) {
                                // NLP
                                rtEventsTableName = "TableEventVector_NLP";
                                tableDataQuality = "EventVectorSetQuality_NLP";
                                db_orig = db_base_dir + "/nlp.db";
                                countReceivedMsgs_NLP = Integer.parseInt(receivedMsgs) - countReceivedMsgsPrev_NLP;
                                countReceivedMsgsPrev_NLP = Integer.parseInt(receivedMsgs);
                                countUsedMsgs_NLP++;
                                countReceivedMsgs = countReceivedMsgs_NLP;
                                countUsedMsgs = countUsedMsgs_NLP;
                        }
                        else if (appID == 2 && userID > 0) {
                                // Audio
                                rtEventsTableName = "TableEventVector_Audio";
                                tableDataQuality = "EventVectorQuality_Audio";
                                db_orig = db_base_dir + "/audio.db";
                                countReceivedMsgs_Audio = Integer.parseInt(receivedMsgs) - countReceivedMsgsPrev_Audio;
                                countReceivedMsgsPrev_Audio = Integer.parseInt(receivedMsgs);
                                countUsedMsgs_Audio++;
                                countReceivedMsgs = countReceivedMsgs_Audio;
                                countUsedMsgs = countUsedMsgs_Audio;
                        }
                        else {
                                // all others Events available in DB
                                rtEventsTableName = "TableEventVector";
                                tableDataQuality = "EventVectorQuality";
                                countReceivedMsgs = Integer.parseInt(receivedMsgs) - countReceivedMsgsPrev;
                                countReceivedMsgsPrev = Integer.parseInt(receivedMsgs);
                                countUsedMsgs++;
                        }

                try {
                        // Users database connection
                        db_conn = DriverManager.getConnection("jdbc:sqlite:" +db_orig);

//Actual invocation of Users DB without "rating" field
			db_stmt = db_conn.prepareStatement("SELECT id, title, country, name, surname FROM user WHERE appID = ? AND userID = ?");
                        db_stmt.setDouble(1, userID);
                        db_stmt.setDouble(2, appID);
                        rs = db_stmt.executeQuery();

// Index updates/inserts
                        String ID = rs.getString(1);
                        String location = rs.getString(2);
                        String country = rs.getString(3);
                        String name = rs.getString(4);
                        String surname = rs.getString(5);

// resultSet adjustment according to the Accuracy and Confidence levels (1 / number of results and multiplied by 100%)
                        accuracyRT = (1/rs.getFetchSize()) * 100;
			confidence = sqrt(accuracyRT*accuracyRT + accuracy*accuracy);

// Collect to DynamoDB items (CandidateSet and CandidateSetQuality)

                        itemRT.put("id", new AttributeValue().withS(placesID));
                        itemRT.put("country", new AttributeValue().withS(country));
                        itemRT.put("name", new AttributeValue().withS(String.valueOf(lat)));
                        itemRT.put("surname", new AttributeValue().withS(String.valueOf(lon)));
                        itemRT.put("query", new AttributeValue().withS(searchQueryAPI));
                        itemRT.put("rating", new AttributeValue().withN(String.valueOf(count)));
                        itemRT.put("title", new AttributeValue().withS(location));
                        itemRT.put("topic", new AttributeValue().withS(getId()));
                        itemRT.put("event", new AttributeValue().withS(activity));
                        itemRT.put("ts", new AttributeValue().withS(dateFormatter.format(new Date())));

                        itemDQ.put("TimeStamp", new AttributeValue().withS(dateFormatter.format(new Date())));
                        itemDQ.put("ReceivedMsgs", new AttributeValue().withN(String.valueOf(countReceivedMsgs)));
                        itemDQ.put("UsedMsgs", new AttributeValue().withN(String.valueOf(countUsedMsgs)));
                        itemDQ.put("Accuracy", new AttributeValue().withN(String.valueOf(count)));
                        itemDQ.put("Timeliness", new AttributeValue().withS(dateFormatter.format(new Date())));
                        itemDQ.put("Completeness", new AttributeValue().withN(String.valueOf(count)));
                        itemDQ.put("Consistency", new AttributeValue().withN(String.valueOf(count)));
                        itemDQ.put("Confidence", new AttributeValue().withN(String.valueOf(count)));
                        itemDQ.put("Privacy", new AttributeValue().withS("anonymised"));

                        PutItemRequest itemRequestRT = new PutItemRequest().withTableName(rtEventsTableName).withItem(itemRT);
                        PutItemRequest itemRequestDQ = new PutItemRequest().withTableName(tableDataQuality).withItem(itemDQ);
                        dynamoDBClient.putItem(itemRequestRT);
                        dynamoDBClient.putItem(itemRequestDQ);
                        itemRT.clear();
                        itemDQ.clear();

                        logger.info("TableEvent set size [{}], last known size [{}] ", countReceivedMsgs, countReceivedMsgsPrev);
                        logger.info("Wrote EventVector to DynamoDB [{}] ", rtEventsTableName);
                        logger.info("Wrote EventVector Quality measurements to DynamoDB [{}] ", tableDataQuality);

// Closing second "try"
                } catch (Exception e) {
        //                logger.error("Cannot close DB file", e);
                }
                finally {
                        try {
                                rs.close();
                                }
                        catch(SQLException e) {
                                logger.error("Cannot close ResultSet", e);
                                }
                        try {
                                db_stmt.close();
                                }
                        catch(SQLException e) {
                                logger.error("Cannot close Statement", e);
                                }
                        try {
                                db_conn.close();
                                }
                        catch(SQLException e) {
                                logger.error("Cannot close DB file", e);
                                }
                        }
// Closing first "try"
        } catch (AmazonServiceException ase) {
                logger.error("Caught an AmazonServiceException, which means your request made it to AWS, but was rejected with an error response for some reason.");
                logger.error("Error Message: " + ase.getMessage());
                logger.error("HTTP Status Code: " + ase.getStatusCode());
                logger.error("AWS Error Code: " + ase.getErrorCode());
                logger.error("Error Type: " + ase.getErrorType());
                logger.error("Request ID: " + ase.getRequestId());

                }

        } // end of if (count == 1)

} // END OF THE BLOCK !!!!!!!!!!!!!!!

    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub


        if (System.getProperty("user.home") == "/home/local-user") {

                System.setProperty("https.proxyHost","proxy");
                System.setProperty("https.proxyPort","8080");
        }

        try {
                // load the sqlite-JDBC driver using the current class loader
                Class.forName("org.sqlite.JDBC");

		// AWS credentials: 
	//      File awsPropsFile = new File(System.getProperty("user.home") + "/AwsCredentials.properties");
        	File awsPropsFile = new File("/home/ec2-user/AwsCredentials.properties");
                if (!awsPropsFile.exists()) {

                        awsPropsFile = new File(System.getProperty("user.home") + "/AwsCredentials.properties");

                        if (!awsPropsFile.exists()) {
                                logger.error(
					"Cannot find AWS Credentials file in this location :[{}]. Make sure it is available at this place and includes AWS credentials (accessKey, secretKey)",
                                        awsPropsFile.getAbsolutePath());
                        }
                }
                awsProperties.load(new FileInputStream(awsPropsFile));

        } catch (Exception e) {
                logger.error("Cannot load sqlite-JDBC driver or there are no AWS credentials in the path", e);
        }
// <<<--- SB
    }

    public void onTime() {
        if (count < threshold) {
            return;
        }
        // Event topicSeenEvent = new Event();
        // topicSeenEvent.put("topic", String.class, getId());
        // topicSeenEvent.put("count", Integer.class, count);
        // topicSeenEvent.put("aggregationKey", String.class, "aggregationValue");

// SB --->>> Extended Event with Location and Place name
	downStream.put(new TopicEvent(getId(), count));

// <<<--- SB
    }

    @Override
    protected void onRemove() {
    }

}
