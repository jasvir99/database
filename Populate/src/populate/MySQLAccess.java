/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package populate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author damandeep
 */
public class MySQLAccess {

    private Connection connect = null;
    private Statement statement = null;
    private PreparedStatement preparedStatement = null;
    private ResultSet resultSet = null;
    
    private static SimpleDateFormat format = new SimpleDateFormat("HH:mm");

    private static String[] mainCats = {"Active Life", "Arts & Entertainment",
        "Automotive", "Car Rental", "Cafes", "Beauty & Spas", "Convenience Stores",
        "Dentists", "Doctors", "Drugstores", "Department Stores", "Education",
        "Event Planning & Services", "Flowers & Gifts", "Food", "Health & Medical",
        "Home Services", "Home & Garden", "Hospitals", "Hotels & Travel",
        "Hardware Stores", "Grocery", "Medical Centers", "Nurseries & Gardening",
        "Nightlife", "Restaurants", "Shopping", "Transportation"};

    public void connect() {
        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.jdbc.Driver");
            // Setup the connection with the DB
            connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/yelp", "root", "dds");

            // Statements allow to issue SQL queries to the database
            statement = connect.createStatement();
        } catch (ClassNotFoundException | SQLException ex) {
            Logger.getLogger(MySQLAccess.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public int cleanTables() throws SQLException, IOException, FileNotFoundException, ParseException {
        Statement stmt = connect.createStatement();
        stmt.execute("SET FOREIGN_KEY_CHECKS=0");
        stmt.close();
        statement.executeUpdate("TRUNCATE table attribute_business_link;");
        int i = 0;
        i = statement.executeUpdate("TRUNCATE table attributes;");

        statement.executeUpdate("TRUNCATE table business;");
        statement.executeUpdate("TRUNCATE table categories;");
        statement.executeUpdate("TRUNCATE table category_business_link;");
        statement.executeUpdate("TRUNCATE table checkins;");
        statement.executeUpdate("TRUNCATE table days;");
        statement.executeUpdate("TRUNCATE table hours;");
        statement.executeUpdate("TRUNCATE table reviews;");
        statement.executeUpdate("TRUNCATE table users;");

        Statement stmt1 = connect.createStatement();
        stmt1.execute("SET FOREIGN_KEY_CHECKS=1");
        stmt1.close();

        insertDays();
        insertParentCategories();

        insertBusiness();
        insertUsers();
insertReviews();
        return i;
    }

    private void insertDays() throws SQLException {
        statement.execute("insert into days (day) values ('Sunday')");
        statement.execute("insert into days (day) values ('Monday')");
        statement.execute("insert into days (day) values ('Tuesday')");
        statement.execute("insert into days (day) values ('Wednesday')");
        statement.execute("insert into days (day) values ('Thursday')");
        statement.execute("insert into days (day) values ('Friday')");
        statement.execute("insert into days (day) values ('Saturday')");

    }

    private void insertParentCategories() throws SQLException {
        statement.execute("insert into categories (cat_name, parent_id) values ('Active Life', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Arts & Entertainment', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Automotive', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Car Rental', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Cafes', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Beauty & Spas', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Convenience Stores', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Dentists', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Doctors', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Drugstores', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Department Stores', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Education', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Event Planning & Services', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Flowers & Gifts', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Food', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Health & Medical', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Home Services', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Home & Garden', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Hospitals', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Hotels & Travel', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Hardware Stores', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Grocery', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Medical Centers', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Nurseries & Gardening', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Nightlife', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Restaurants', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Shopping', NULL)");
        statement.execute("insert into categories (cat_name, parent_id) values ('Transportation', NULL)");
    }

    public void readDataBase() throws Exception {
        try {
            // This will load the MySQL driver, each DB has its own driver
            // Result set get the result of the SQL query
            resultSet = statement
                    .executeQuery("show tables;");
            writeResultSet(resultSet);

            // PreparedStatements can use variables and are more efficient
//      preparedStatement = connect
//          .prepareStatement("insert into  feedback.comments values (default, ?, ?, ?, ? , ?, ?)");
//      // "myuser, webpage, datum, summery, COMMENTS from feedback.comments");
//      // Parameters start with 1
//      preparedStatement.setString(1, "Test");
//      preparedStatement.setString(2, "TestEmail");
//      preparedStatement.setString(3, "TestWebpage");
//      preparedStatement.setDate(4, new java.sql.Date(2009, 12, 11));
//      preparedStatement.setString(5, "TestSummary");
//      preparedStatement.setString(6, "TestComment");
//      preparedStatement.executeUpdate();
//
//      preparedStatement = connect
//          .prepareStatement("SELECT myuser, webpage, datum, summery, COMMENTS from feedback.comments");
//      resultSet = preparedStatement.executeQuery();
//      writeResultSet(resultSet);
//
//      // Remove again the insert comment
//      preparedStatement = connect
//      .prepareStatement("delete from feedback.comments where myuser= ? ; ");
//      preparedStatement.setString(1, "Test");
//      preparedStatement.executeUpdate();
//      
//      resultSet = statement
//      .executeQuery("select * from feedback.comments");
//      writeMetaData(resultSet);
        } catch (Exception e) {
            throw e;
        } finally {
            close();
        }

    }

    private void insertBusiness() throws FileNotFoundException, IOException, SQLException, ParseException {
        try {
            BufferedReader br = null;
            String fileName = "file:///Volumes/DATA/Downloads/yelp_business.json";
            URI fileUri = new URI(fileName);
            br = new BufferedReader(new FileReader(new File(fileUri)));
            String line;
            while ((line = br.readLine()) != null) {
                JSONObject obj = new JSONObject(line);
//                Iterator<?> keys = obj.keys();
//              
//                while(keys.hasNext()) {
//                    System.out.print(keys.next()+"\t");
//                }

                preparedStatement = connect
                        .prepareStatement("insert into business values (?, ?, ?, ?, ? , ?, ?, ?, ?, ?, ?, default)");
                // Parameters start with 1
                preparedStatement.setString(1, obj.getString("business_id"));
                preparedStatement.setString(2, obj.getString("full_address"));
                preparedStatement.setBoolean(3, obj.getBoolean("open"));
                preparedStatement.setString(4, obj.getString("city"));
                preparedStatement.setString(5, obj.getString("state"));
                preparedStatement.setDouble(6, obj.getDouble("latitude"));
                preparedStatement.setDouble(7, obj.getDouble("longitude"));
                preparedStatement.setInt(8, obj.getInt("review_count"));
                preparedStatement.setString(9, obj.getString("name"));
                preparedStatement.setString(10, obj.getJSONArray("neighborhoods").toString());
                preparedStatement.setFloat(11, (float) obj.getDouble("stars"));
                preparedStatement.executeUpdate();

                JSONArray catArray = obj.getJSONArray("categories");
                String[] cArray = new String[catArray.length()];
                for (int x = 0; x < catArray.length(); x++) {
                    cArray[x] = catArray.getString(x);
                }

                boolean isSet = false;

                List<Integer> parentCatList = new ArrayList<>();
                List<Integer> rmList = new ArrayList<>();

                for (int x = 0; x < mainCats.length; x++) {
                    for (int y = 0; y < cArray.length; y++) {
                        if (mainCats[x].equalsIgnoreCase(cArray[y])) {
                            parentCatList.add((x + 1));
                            rmList.add(y);
                            isSet = true;
                        }
                    }
                }

                if (isSet) {
                    for (int b = 0; b < parentCatList.size(); b++) {
                        preparedStatement = connect.prepareStatement("insert ignore into category_business_link (business_id, cat_id) values (?,?);");
                        preparedStatement.setString(1, obj.getString("business_id"));
                        preparedStatement.setInt(2, parentCatList.get(b));
                        preparedStatement.executeUpdate();

                        List<String> cList = new ArrayList<String>();
                        Collections.addAll(cList, cArray);
                        cList.remove(rmList.get(b));

                        for (int i = 0; i < cList.size(); i++) {

                            preparedStatement = connect.prepareStatement("select cat_id from categories where cat_name = ? AND parent_id = ?");
                            preparedStatement.setString(1, cList.get(i));
                            preparedStatement.setInt(2, parentCatList.get(b));
                            resultSet = preparedStatement.executeQuery();
                            if (resultSet.first()) {

                            } else {
                                preparedStatement = connect.prepareStatement("insert ignore into categories (cat_name, parent_id) values (?,?);");
                                preparedStatement.setString(1, cList.get(i));
                                preparedStatement.setInt(2, parentCatList.get(b));
                                preparedStatement.executeUpdate();
                            }

                            preparedStatement = connect.prepareStatement("select cat_id from categories where cat_name = ? AND parent_id = ?");
                            preparedStatement.setString(1, cList.get(i));
                            preparedStatement.setInt(2, parentCatList.get(b));
                            resultSet = preparedStatement.executeQuery();
                            resultSet.first();
                            int idd = resultSet.getInt(1);
                            preparedStatement = connect.prepareStatement("insert ignore into category_business_link (business_id, cat_id) values (?,?);");
                            preparedStatement.setString(1, obj.getString("business_id"));
                            preparedStatement.setInt(2, idd);
                            preparedStatement.executeUpdate();
                        }
                    }

                } else {
                    printArray(cArray);
                }

                JSONObject attrObj = obj.getJSONObject("attributes");
                Iterator<String> keys = attrObj.keys();

                while (keys.hasNext()) {
                    String key = keys.next();
                    if (attrObj.get(key) instanceof Boolean) {
                        preparedStatement = connect.prepareStatement("insert ignore into attributes (attribute) values (?);");
                        preparedStatement.setString(1, key);
                        preparedStatement.executeUpdate();
                        if (attrObj.getBoolean(key)) {
                            preparedStatement = connect.prepareStatement("select id from attributes where attribute = ? ");
                            preparedStatement.setString(1, key);
                            resultSet = preparedStatement.executeQuery();
                            resultSet.first();
                            int idd = resultSet.getInt(1);
                            preparedStatement = connect.prepareStatement("insert into attribute_business_link (business_id, attr_id) values (?,?);");
                            preparedStatement.setString(1, obj.getString("business_id"));
                            preparedStatement.setInt(2, idd);
                            preparedStatement.executeUpdate();
                        }

                    } else if (attrObj.get(key) instanceof JSONObject) {
                        JSONObject subObject = attrObj.getJSONObject(key);
                        Iterator<String> subKeys = subObject.keys();
                        while (subKeys.hasNext()) {
                            String skey = subKeys.next();
                            preparedStatement = connect.prepareStatement("insert ignore into attributes (attribute) values (?);");
                            preparedStatement.setString(1, key + "_" + skey);
                            preparedStatement.executeUpdate();
                            if (subObject.getBoolean(skey)) {
                                preparedStatement = connect.prepareStatement("select id from attributes where attribute = ? ");
                                preparedStatement.setString(1, key + "_" + skey);
                                resultSet = preparedStatement.executeQuery();
                                resultSet.first();
                                int idd = resultSet.getInt(1);
                                preparedStatement = connect.prepareStatement("insert into attribute_business_link (business_id, attr_id) values (?,?);");
                                preparedStatement.setString(1, obj.getString("business_id"));
                                preparedStatement.setInt(2, idd);
                                preparedStatement.executeUpdate();
                            }
                        }
                    } else if (attrObj.get(key) instanceof Integer) {
                        int xyz = attrObj.getInt(key);
                        preparedStatement = connect.prepareStatement("insert ignore into attributes (attribute) values (?);");
                        preparedStatement.setString(1, key + "_" + xyz);
                        preparedStatement.executeUpdate();
                        preparedStatement = connect.prepareStatement("select id from attributes where attribute = ? ");
                        preparedStatement.setString(1, key + "_" + xyz);
                        resultSet = preparedStatement.executeQuery();
                        resultSet.first();
                        int idd = resultSet.getInt(1);
                        preparedStatement = connect.prepareStatement("insert into attribute_business_link (business_id, attr_id) values (?,?);");
                        preparedStatement.setString(1, obj.getString("business_id"));
                        preparedStatement.setInt(2, idd);
                        preparedStatement.executeUpdate();
                    } else {
                        String xyz = attrObj.getString(key);
                        preparedStatement = connect.prepareStatement("insert ignore into attributes (attribute) values (?);");
                        preparedStatement.setString(1, key + "_" + xyz);
                        preparedStatement.executeUpdate();
                        preparedStatement = connect.prepareStatement("select id from attributes where attribute = ? ");
                        preparedStatement.setString(1, key + "_" + xyz);
                        resultSet = preparedStatement.executeQuery();
                        resultSet.first();
                        int idd = resultSet.getInt(1);
                        preparedStatement = connect.prepareStatement("insert into attribute_business_link (business_id, attr_id) values (?,?);");
                        preparedStatement.setString(1, obj.getString("business_id"));
                        preparedStatement.setInt(2, idd);
                        preparedStatement.executeUpdate();
                    }

                }
                
                JSONObject hoursObj = obj.getJSONObject("hours");
                Iterator<String> hKeys = hoursObj.keys();
                while(hKeys.hasNext()) {
                    String day = hKeys.next();
                    JSONObject dObject = hoursObj.getJSONObject(day);
                    String open = dObject.getString("open");
                    String close = dObject.getString("close");
                    Date oDate = format.parse(open);
                    Time oTime = new Time(oDate.getTime());
                    Date cDate = format.parse(close);
                    Time cTime = new Time(cDate.getTime());
                    
                    preparedStatement = connect.prepareStatement("select day_id from days where day = ? ");
                        preparedStatement.setString(1, day);
                        resultSet = preparedStatement.executeQuery();
                        resultSet.first();
                        int idd = resultSet.getInt(1);
                        preparedStatement = connect.prepareStatement("insert into hours(business_id,day_id,open,close) values (?,?,?,?);");
                        preparedStatement.setString(1, obj.getString("business_id"));
                        preparedStatement.setInt(2, idd);
                        preparedStatement.setTime(3, oTime);
                        preparedStatement.setTime(4, cTime);
                        preparedStatement.executeUpdate();
                    
                }

            }
        } catch (URISyntaxException ex) {
            Logger.getLogger(MySQLAccess.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void writeMetaData(ResultSet resultSet) throws SQLException {
        //   Now get some metadata from the database
        // Result set get the result of the SQL query

        System.out.println("The columns in the table are: ");

        System.out.println("Table: " + resultSet.getMetaData().getTableName(1));
        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
            System.out.println("Column " + i + " " + resultSet.getMetaData().getColumnName(i));
        }
    }
    
    private void insertUsers() {
        try {
            BufferedReader br = null;
            String fileName = "file:///Volumes/DATA/Downloads/yelp_user.json";
            URI fileUri = new URI(fileName);
            br = new BufferedReader(new FileReader(new File(fileUri)));
            String line;
            while ((line = br.readLine()) != null) {
                JSONObject obj = new JSONObject(line); 
                JSONObject votesObj = obj.getJSONObject("votes");
                preparedStatement = connect
                        .prepareStatement("insert into users values (?, ?, ?, ?, ? , ?, ?, ?, default)");
                // Parameters start with 1
                preparedStatement.setString(1, obj.getString("user_id"));
                preparedStatement.setInt(2, votesObj.getInt("funny"));
                preparedStatement.setInt(3, votesObj.getInt("useful"));
                preparedStatement.setInt(4, votesObj.getInt("cool"));
                preparedStatement.setInt(5, obj.getInt("review_count"));
                preparedStatement.setString(6, obj.getString("name"));
                preparedStatement.setInt(7, obj.getInt("fans"));
                preparedStatement.setDouble(8, obj.getDouble("average_stars"));
                preparedStatement.executeUpdate();
            }
        } catch (URISyntaxException ex) {
            Logger.getLogger(MySQLAccess.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(MySQLAccess.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(MySQLAccess.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(MySQLAccess.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void insertReviews() throws ParseException {
        try {
            BufferedReader br = null;
            SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd");
            String fileName = "file:///Volumes/DATA/Downloads/yelp_review.json";
            URI fileUri = new URI(fileName);
            br = new BufferedReader(new FileReader(new File(fileUri)));
            String line;
            while ((line = br.readLine()) != null) {
                JSONObject obj = new JSONObject(line); 
                JSONObject votesObj = obj.getJSONObject("votes");
                preparedStatement = connect
                        .prepareStatement("insert into reviews values (?, ?, ?, ?, ? , ?, ?, ? ,?, default)");
                // Parameters start with 1
                preparedStatement.setString(1, obj.getString("review_id"));
                preparedStatement.setString(2, obj.getString("user_id"));
                preparedStatement.setString(3, obj.getString("business_id"));
                preparedStatement.setInt(5, votesObj.getInt("funny"));
                preparedStatement.setInt(4, votesObj.getInt("useful"));
                preparedStatement.setInt(6, votesObj.getInt("cool"));
                preparedStatement.setInt(7, obj.getInt("stars"));
                preparedStatement.setDate(8, new java.sql.Date(sdf.parse(obj.getString("date")).getTime()));
                preparedStatement.setString(9,obj.getString("text"));
                preparedStatement.executeUpdate();
            }
        } catch (URISyntaxException ex) {
            Logger.getLogger(MySQLAccess.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(MySQLAccess.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(MySQLAccess.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(MySQLAccess.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void writeResultSet(ResultSet resultSet) throws SQLException {
        // ResultSet is initially before the first data set
        while (resultSet.next()) {
            // It is possible to get the columns via name
            // also possible to get the columns via the column number
            // which starts at 1
            // e.g. resultSet.getSTring(2);
            System.out.println(resultSet.getString(1));

//      String user = resultSet.getString("myuser");
//      String website = resultSet.getString("webpage");
//      String summery = resultSet.getString("summery");
//      Date date = resultSet.getDate("datum");
//      String comment = resultSet.getString("comments");
//      System.out.println("User: " + user);
//      System.out.println("Website: " + website);
//      System.out.println("Summery: " + summery);
//      System.out.println("Date: " + date);
//      System.out.println("Comment: " + comment);
        }
    }

    // You need to close the resultSet
    private void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }

            if (statement != null) {
                statement.close();
            }

            if (connect != null) {
                connect.close();
            }
        } catch (Exception e) {

        }
    }

    private void printArray(String[] array) {
        System.out.print("Array:\t");
        for (int i = 0; i < array.length; i++) {
            System.out.print(array[i] + "\t");
        }
        System.out.print("\n");
    }
}
