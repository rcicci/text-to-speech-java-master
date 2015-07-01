package com.ibm.cloudoe.samples;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ibm.db2.jcc.DB2SimpleDataSource;

import com.ibm.nosql.json.api.BasicDBList;
import com.ibm.nosql.json.api.BasicDBObject;
import com.ibm.nosql.json.util.JSON;

// @author PvR, IBM 2013
// @version 0.3

public class DbHelper {
    private static Logger logger = Logger.getLogger(DbHelper.class.getName());
    // set defaults
    private String databaseHost = "";
    private int port = 50000;
    private String databaseName = "";
    private String user = "";
    private String password = "";
    private String url = "";

    public DbHelper() {
    }

    private boolean processVCAP() {
        // VCAP_SERVICES is a system environment variable
        // Parse it to obtain the for DB2 connection info
        String VCAP_SERVICES = System.getenv("VCAP_SERVICES");
        logger.log(Level.INFO, "VCAP_SERVICES content: " + VCAP_SERVICES);

        if (VCAP_SERVICES != null) {
            // parse the VCAP JSON structure
            BasicDBObject obj = (BasicDBObject) JSON.parse(VCAP_SERVICES);
            String thekey = null;
            Set<String> keys = obj.keySet();
            logger.log(Level.INFO, "Searching through VCAP keys");
            // Look for the VCAP key that holds the SQLDB information
            for (String eachkey : keys) {
                logger.log(Level.INFO, "Key is: " + eachkey);
                // Just in case the service name gets changed to lower case in the future, use toUpperCase
                if (eachkey.toUpperCase().contains("SQLDB")) {
                    thekey = eachkey;
                }
            }
            if (thekey == null) {
                logger.log(Level.INFO, "Cannot find any SQLDB service in the VCAP; exiting");
                return false;
            }
            BasicDBList list = (BasicDBList) obj.get(thekey);
            obj = (BasicDBObject) list.get("0");
            logger.log(Level.INFO, "Service found: " + obj.get("name"));
            // parse all the credentials from the vcap env variable
            obj = (BasicDBObject) obj.get("credentials");
            databaseHost = (String) obj.get("host");
            databaseName = (String) obj.get("db");
            port = (Integer)obj.get("port");
            user = (String) obj.get("username");
            password = (String) obj.get("password");
            url = (String) obj.get("jdbcurl");
        } else {
            logger.log(Level.INFO, "VCAP_SERVICES is null");
            return false;
        }
        logger.log(Level.INFO, "database host: " + databaseHost);
        logger.log(Level.INFO, "database port: " + port);
        logger.log(Level.INFO, "database name: " + databaseName);
        logger.log(Level.INFO, "username: " + user);
        logger.log(Level.INFO, "password: " + password);
        logger.log(Level.INFO, "url: " + url);
        return true;
    }

    public String getValue() {
        logger.log(Level.INFO, "IBM SQL Database, Java Demo Application using DB2 drivers");
        logger.log(Level.INFO, "Servlet: " + this.getClass().getName());
//        logger.log(Level.INFO, "Host IP:" + InetAddress.getLocalHost().getHostAddress());

        String value = null;

        // process the VCAP env variable and set all the global connection parameters
        if (processVCAP()) {

            // Connect to the Database
            Connection con;
            try {
                logger.log(Level.INFO, "Connecting to the database");
                DB2SimpleDataSource dataSource = new DB2SimpleDataSource();
                dataSource.setServerName(databaseHost);
                dataSource.setPortNumber(port);
                dataSource.setDatabaseName(databaseName);
                dataSource.setUser(user);
                dataSource.setPassword (password);
                dataSource.setDriverType(4);
                con=dataSource.getConnection();
                con.setAutoCommit(false);
            } catch (SQLException e) {
                logger.log(Level.INFO, "Error connecting to database");
                logger.log(Level.INFO, "SQL Exception: " + e);
                return null;
            }

            // Try out some dynamic SQL Statements
            Statement stmt = null;
            String tableName = "PROPERTIES";
            String sqlStatement = "";

            // Execute some SQL statements on the table: Insert, Select and Delete
            try {
                stmt = con.createStatement();
                sqlStatement = "SELECT VALUE FROM " + tableName + " WHERE NAME = 'property1'";
                ResultSet rs = stmt.executeQuery(sqlStatement);
                logger.log(Level.INFO, "Executing: " + sqlStatement);

                // Process the result set
                while (rs.next()) {
                    value = rs.getString(1);
                    logger.log(Level.INFO, "  Found value: " + value);
                }
                // Close the ResultSet
                rs.close();
            } catch (SQLException e) {
                logger.log(Level.INFO, "Error executing:" + sqlStatement);
                logger.log(Level.INFO, "SQL Exception: " + e);
            }

            // Close everything off
            try {
                // Close the Statement
                if (stmt != null) {
                    stmt.close();
                }
                // Connection must be on a unit-of-work boundary to allow close
                if (con != null) {
                    con.commit();
                    // Close the connection
                    con.close();
                }
                logger.log(Level.INFO, "Finished");

            } catch (SQLException e) {
                logger.log(Level.INFO, "Error closing things off");
                logger.log(Level.INFO, "SQL Exception: " + e);
            }
        }
        System.out.close();
        return value;
    }
}