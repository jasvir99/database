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
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Class.forName("com.mysql.jdbc.Driver");
            // Setup the connection with the DB
            connect = DriverManager
                    .getConnection("jdbc:oracle:thin:@localhost:1521:sysdba", "c##jasvir", "password");

            // Statements allow to issue SQL queries to the database
            statement = connect.createStatement();
        } catch (ClassNotFoundException | SQLException ex) {
            Logger.getLogger(MySQLAccess.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public int cleanTables() throws SQLException, IOException, FileNotFoundException, ParseException {
        Statement stmt = connect.createStatement();
        stmt.execute("ALTER TABLE categories DISABLE CONSTRAINT parent_cat_const");
        stmt.execute("ALTER TABLE category_business_link DISABLE CONSTRAINT category_business_link_ibfk_2");
        stmt.execute("ALTER TABLE category_business_link DISABLE CONSTRAINT category_business_link_ibfk_1");
        stmt.execute("ALTER TABLE checkins DISABLE CONSTRAINT checkins_ibfk_1");
        stmt.execute("ALTER TABLE checkins DISABLE CONSTRAINT checkins_ibfk_2");
        stmt.execute("ALTER TABLE hours DISABLE CONSTRAINT b_id_const");
        stmt.execute("ALTER TABLE hours DISABLE CONSTRAINT day_id_const");
        stmt.execute("ALTER TABLE reviews DISABLE CONSTRAINT reviews_ibfk_1");
        stmt.execute("ALTER TABLE reviews DISABLE CONSTRAINT reviews_ibfk_2");
        stmt.execute("ALTER TABLE attribute_business_link DISABLE CONSTRAINT attribute_business_link_ibfk_1");
        stmt.execute("ALTER TABLE attribute_business_link DISABLE CONSTRAINT attribute_business_link_ibfk_2");
        stmt.close();
       
        int i = 1;
        //i = 
        statement.executeUpdate("TRUNCATE table attribute_business_link");
        statement.executeUpdate("TRUNCATE table attributes");
        statement.executeUpdate("TRUNCATE table business");
        statement.executeUpdate("TRUNCATE table categories");
        statement.executeUpdate("TRUNCATE table category_business_link");
        statement.executeUpdate("TRUNCATE table checkins");
        statement.executeUpdate("TRUNCATE table days");
        statement.executeUpdate("TRUNCATE table hours");
        statement.executeUpdate("TRUNCATE table reviews");
        statement.executeUpdate("TRUNCATE table users");

        Statement stmt1 = connect.createStatement();
        stmt1.execute("ALTER TABLE categories ENABLE CONSTRAINT parent_cat_const");
        stmt1.execute("ALTER TABLE category_business_link ENABLE CONSTRAINT category_business_link_ibfk_2");
        stmt1.execute("ALTER TABLE category_business_link ENABLE CONSTRAINT category_business_link_ibfk_1");
        stmt1.execute("ALTER TABLE checkins ENABLE CONSTRAINT checkins_ibfk_1");
        stmt1.execute("ALTER TABLE checkins ENABLE CONSTRAINT checkins_ibfk_2");
        stmt1.execute("ALTER TABLE hours ENABLE CONSTRAINT b_id_const");
        stmt1.execute("ALTER TABLE hours ENABLE CONSTRAINT day_id_const");
        stmt1.execute("ALTER TABLE reviews ENABLE CONSTRAINT reviews_ibfk_1");
        stmt1.execute("ALTER TABLE reviews ENABLE CONSTRAINT reviews_ibfk_2");
        stmt1.execute("ALTER TABLE attribute_business_link ENABLE CONSTRAINT attribute_business_link_ibfk_1");
        stmt1.execute("ALTER TABLE attribute_business_link ENABLE CONSTRAINT attribute_business_link_ibfk_2");
        stmt1.close();

        insertDays();
        insertParentCategories();

        insertBusiness();
        insertUsers();
insertReviews();
        return i;
    }

    private void insertDays() throws SQLException {
        statement.execute("insert into days (day_id,day) values (1,'Sunday')");
        statement.execute("insert into days (day_id,day) values (2,'Monday')");
        statement.execute("insert into days (day_id,day) values (3,'Tuesday')");
        statement.execute("insert into days (day_id,day) values (4,'Wednesday')");
        statement.execute("insert into days (day_id,day) values (5,'Thursday')");
        statement.execute("insert into days (day_id,day) values (6,'Friday')");
        statement.execute("insert into days (day_id,day) values (7,'Saturday')");

    }

    private void insertParentCategories() throws SQLException {
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (1,'Active Life', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (2,'Arts & Entertainment', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (3,'Automotive', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (4,'Car Rental', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (5,'Cafes', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (6,'Beauty & Spas', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (7,'Convenience Stores', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (8,'Dentists', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (9,'Doctors', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (10,'Drugstores', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (11,'Department Stores', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (12,'Education', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (13,'Event Planning & Services', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (14,'Flowers & Gifts', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (15,'Food', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (16,'Health & Medical', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (17,'Home Services', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (18,'Home & Garden', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (19,'Hospitals', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (20,'Hotels & Travel', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (21,'Hardware Stores', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (22,'Grocery', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (23,'Medical Centers', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (24,'Nurseries & Gardening', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (25,'Nightlife', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (26,'Restaurants', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (27,'Shopping', NULL)");
        statement.execute("insert into categories (cat_id, cat_name, parent_id) values (28,'Transportation', NULL)");
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
           // ArrayList cat_id = 1;
            BufferedReader br = null;
            String fileName = "file:/Users/jass/Downloads/YelpDataset/yelp_business.json";
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
                preparedStatement.close();

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
                        preparedStatement.close();
                        preparedStatement = connect.prepareStatement("insert /*+ ignore_row_on_dupkey_index(category_business_link, id) */ into category_business_link (id,business_id, cat_id) values (?,?,?)");
                        preparedStatement.setInt(1, b);
                        preparedStatement.setString(2, obj.getString("business_id"));
                        preparedStatement.setInt(3, parentCatList.get(b));
                        preparedStatement.executeUpdate();
                        preparedStatement.close();

                        List<String> cList = new ArrayList<String>();
                        Collections.addAll(cList, cArray);
                        cList.remove(rmList.get(b));

                        for (int i = 0; i < cList.size(); i++) {
                            preparedStatement.close();
                            preparedStatement = connect.prepareStatement("select cat_id from categories where cat_name = ? AND parent_id = ?");
                            preparedStatement.setString(1, cList.get(i));
                            preparedStatement.setInt(2, parentCatList.get(b));
                            resultSet = preparedStatement.executeQuery();
                            
                            if (resultSet.isFirst()) {
                              preparedStatement.close();
                              resultSet.close();

                            } else {
                                preparedStatement.close();
                                resultSet.close();
                                preparedStatement = connect.prepareStatement("insert /*+ ignore_row_on_dupkey_index(categories, cat_id) */ into categories (cat_id,cat_name, parent_id) values (?,?,?)");
                                preparedStatement.setInt(1, i);
                                preparedStatement.setString(2, cList.get(i));
                                preparedStatement.setInt(3, parentCatList.get(b));
                                preparedStatement.executeUpdate();
                                preparedStatement.close();
                            }
                            preparedStatement.close();
                            preparedStatement = connect.prepareStatement("select cat_id from categories where cat_name = ? AND parent_id = ?");
                            preparedStatement.setString(1, cList.get(i));
                            preparedStatement.setInt(2, parentCatList.get(b));
                            resultSet = preparedStatement.executeQuery();
                            
                            if(!resultSet.next())
                            {
                                preparedStatement.close();
                                resultSet.close();
                            }
                            else{
                                
                                int idd = resultSet.getInt(1);
                                preparedStatement.close();
                                preparedStatement = connect.prepareStatement("insert /*+ ignore_row_on_dupkey_index(business_id, cat_id) */ into category_business_link (id,business_id, cat_id) values (?,?,?)");
                                preparedStatement.setInt(1, i);
                                preparedStatement.setString(2, obj.getString("business_id"));
                                preparedStatement.setInt(3, idd);
                                preparedStatement.executeUpdate();
                                preparedStatement.close();
                            }
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
                        preparedStatement.close();
                        preparedStatement = connect.prepareStatement("select max(id) from attributes");
                        resultSet = preparedStatement.executeQuery();
                        resultSet.next();
                        int atr_id = resultSet.getInt(1)+1;
                        preparedStatement.close();
                        resultSet.close();
                        preparedStatement = connect.prepareStatement("insert /*+ ignore_row_on_dupkey_index(attributes, id) */ into attributes (id, attribute) values (?,?)");
                        preparedStatement.setInt(1,atr_id);
                        System.out.println("1:" + atr_id);
                        preparedStatement.setString(2, key);
                        try
                        {
                            preparedStatement.executeUpdate();
                            preparedStatement.close();
                        } catch (Exception e)
                        {
                            
                        }
                        if (attrObj.getBoolean(key)) {
                            preparedStatement.close();
                            preparedStatement = connect.prepareStatement("select id from attributes where attribute = ? ");
                            preparedStatement.setString(1, key);
                            resultSet = preparedStatement.executeQuery();
                            
                            if(!resultSet.next())
                            {
                                preparedStatement.close();
                                resultSet.close();
                            }
                            else{
                                
                            int idd = resultSet.getInt(1);
                            preparedStatement.close();
                            preparedStatement = connect.prepareStatement("select max(id) from attribute_business_link");
                            resultSet = preparedStatement.executeQuery();
                            
                            resultSet.next();
                            int att_bus_id = resultSet.getInt(1) + 1;
                            preparedStatement.close();
                            resultSet.close();
                            preparedStatement = connect.prepareStatement("insert into attribute_business_link (id, business_id, attr_id) values (?,?,?)");
                            preparedStatement.setInt(1,att_bus_id);
                            preparedStatement.setString(2, obj.getString("business_id"));
                            preparedStatement.setInt(3, idd);
                            preparedStatement.executeUpdate();
                            preparedStatement.close();
                            }
                        }

                    } else if (attrObj.get(key) instanceof JSONObject) {
                        JSONObject subObject = attrObj.getJSONObject(key);
                        Iterator<String> subKeys = subObject.keys();
                        while (subKeys.hasNext()) {
                            String skey = subKeys.next();
                            preparedStatement.close();
                            preparedStatement = connect.prepareStatement("select max(id) from attributes");
                            resultSet = preparedStatement.executeQuery();
                            resultSet.next();
                            System.out.println("2:" + resultSet.getInt(1));
                            int atr_id = resultSet.getInt(1)+1;
                            preparedStatement.close();
                            resultSet.close();
                            preparedStatement = connect.prepareStatement("insert /*+ ignore_row_on_dupkey_index(attributes, id) */ into attributes (id, attribute) values (?,?)");
                            preparedStatement.setInt(1,atr_id);
                            System.out.println("3:" + atr_id);
                            preparedStatement.setString(2, key + "_" + skey);
                            try{
                                preparedStatement.executeUpdate();
                                preparedStatement.close();
                            } catch (Exception e)
                            {
                                
                            }
                            if (subObject.getBoolean(skey)) {
                                resultSet.close();
                                preparedStatement.close();
                                preparedStatement = connect.prepareStatement("select id from attributes where attribute = ? ");
                                preparedStatement.setString(1, key + "_" + skey);
                                resultSet = preparedStatement.executeQuery();
                                
                                if(!resultSet.next())
                                {
                                    preparedStatement.close();
                                    resultSet.close();
                                }
                                else{
                                int idd = resultSet.getInt(1);
                                resultSet.close();
                                preparedStatement.close();
                                preparedStatement = connect.prepareStatement("select max(id) from attribute_business_link");
                                resultSet = preparedStatement.executeQuery();
                                
                                resultSet.next();
                                int att_bus_id = resultSet.getInt(1) + 1;
                                preparedStatement.close();
                                resultSet.close();
                                preparedStatement = connect.prepareStatement("insert into attribute_business_link (id,business_id, attr_id) values (?,?,?)");
                                preparedStatement.setInt(1, att_bus_id);
                                preparedStatement.setString(2, obj.getString("business_id"));
                                preparedStatement.setInt(3, idd);
                                preparedStatement.executeUpdate();
                                preparedStatement.close();
                                }
                            }
                        }
                    } else if (attrObj.get(key) instanceof Integer) {
                        int xyz = attrObj.getInt(key);
                        preparedStatement = connect.prepareStatement("select max(id) from attributes");
                        resultSet = preparedStatement.executeQuery();
                        
                        resultSet.next();
                        System.out.println("2:" + resultSet.getInt(1));
                        int atr_id = resultSet.getInt(1)+1;
                        preparedStatement.close();
                        resultSet.close();
                        preparedStatement = connect.prepareStatement("insert /*+ ignore_row_on_dupkey_index(attributes, id) */ into attributes (id, attribute) values (?,?)");
                        preparedStatement.setInt(1,atr_id);
                        preparedStatement.setString(2, key + "_" + xyz);
                        try{
                            preparedStatement.executeUpdate();
                            preparedStatement.close();
                        }
                        catch (Exception e)
                        {
                            
                        }
                        preparedStatement.close();
                        preparedStatement = connect.prepareStatement("select id from attributes where attribute = ? ");
                        preparedStatement.setString(1, key + "_" + xyz);
                        resultSet = preparedStatement.executeQuery();
                        
                        if(!resultSet.next())
                        {
                            preparedStatement.close();
                            resultSet.close();
                        }
                        else{
                            
                        int idd = resultSet.getInt(1);
                        preparedStatement = connect.prepareStatement("insert into attribute_business_link (business_id, attr_id) values (?,?);");
                        preparedStatement.setString(1, obj.getString("business_id"));
                        preparedStatement.setInt(2, idd);
                        preparedStatement.executeUpdate();
                        preparedStatement.close();
                        }
                    } else {
                        String xyz = attrObj.getString(key);
                        preparedStatement.close();
                        resultSet.close();
                        preparedStatement = connect.prepareStatement("select max(id) from attributes");
                        resultSet = preparedStatement.executeQuery();
                        
                        resultSet.next();
                        System.out.println("4:" + resultSet.getInt(1));
                        int atr_id = resultSet.getInt(1)+1;
                        preparedStatement.close();
                        resultSet.close();
                        preparedStatement = connect.prepareStatement("insert /*+ ignore_row_on_dupkey_index(attributes, id) */  into attributes (id, attribute) values (?,?)");
                        preparedStatement.setInt(1,atr_id);
                        preparedStatement.setString(2, key + "_" + xyz);
                        try
                        {
                            preparedStatement.executeUpdate();
                            preparedStatement.close();
                        } catch (Exception e)
                        {
                            
                        }
                        preparedStatement.close();
                        preparedStatement = connect.prepareStatement("select id from attributes where attribute = ? ");
                        preparedStatement.setString(1, key + "_" + xyz);
                        resultSet = preparedStatement.executeQuery();
                        
                        if(!resultSet.next())
                        {
                            preparedStatement.close();
                            resultSet.close();
                        }
                        else{
                            
                        int idd = resultSet.getInt(1);
                        preparedStatement.close();
                        preparedStatement = connect.prepareStatement("insert into attribute_business_link (business_id, attr_id) values (?,?);");
                        preparedStatement.setString(1, obj.getString("business_id"));
                        preparedStatement.setInt(2, idd);
                        preparedStatement.executeUpdate();
                        preparedStatement.close();
                        }
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
                    preparedStatement.close();
                    preparedStatement = connect.prepareStatement("select day_id from days where day = ? ");
                        preparedStatement.setString(1, day);
                        resultSet = preparedStatement.executeQuery();
                        
                        resultSet.next();
                        int idd = resultSet.getInt(1);
                        preparedStatement.close();
                        resultSet.close();
                        preparedStatement = connect.prepareStatement("select max(id) from hours");
                        resultSet = preparedStatement.executeQuery();
                        
                        resultSet.next();
                        int hour_id = resultSet.getInt(1);
                        preparedStatement.close();
                        resultSet.close();
                        preparedStatement = connect.prepareStatement("insert into hours(id,business_id,day_id,open,close) values (?,?,?,?,?)");
                        preparedStatement.setInt(1,hour_id);
                        preparedStatement.setString(2, obj.getString("business_id"));
                        preparedStatement.setInt(3, idd);
                        preparedStatement.setTime(4, oTime);
                        preparedStatement.setTime(5, cTime);
                        preparedStatement.executeUpdate();
                        preparedStatement.close();
                    
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
            String fileName = "file:/Users/jass/Downloads/YelpDataset/yelp_user.json";
            URI fileUri = new URI(fileName);
            br = new BufferedReader(new FileReader(new File(fileUri)));
            String line;
            while ((line = br.readLine()) != null) {
                JSONObject obj = new JSONObject(line); 
                JSONObject votesObj = obj.getJSONObject("votes");
                preparedStatement.close();
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
            System.out.println("reviews");
            BufferedReader br = null;
            SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd");
            String fileName = "file:/Users/jass/Downloads/YelpDataset/yelp_review.json";
            URI fileUri = new URI(fileName);
            br = new BufferedReader(new FileReader(new File(fileUri)));
            String line;
            while ((line = br.readLine()) != null) {
                JSONObject obj = new JSONObject(line); 
                JSONObject votesObj = obj.getJSONObject("votes");
                preparedStatement.close();
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
                preparedStatement.close();
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
