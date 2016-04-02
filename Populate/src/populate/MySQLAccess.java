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
        stmt = connect.createStatement();
        //stmt.executeUpdate("TRUNCATE table attribute_business_link");
        //stmt.executeUpdate("TRUNCATE table attributes");
       // stmt.executeUpdate("TRUNCATE table business");
       // stmt.executeUpdate("TRUNCATE table categories");
       //// stmt.executeUpdate("TRUNCATE table category_business_link");
       // stmt.executeUpdate("TRUNCATE table checkins");
       // stmt.executeUpdate("TRUNCATE table days");
       // stmt.executeUpdate("TRUNCATE table hours");
       // stmt.executeUpdate("TRUNCATE table reviews");
       // stmt.executeUpdate("TRUNCATE table users");
        stmt.close();
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

       // insertDays();
        //System.out.println("Days inserted");
        //insertParentCategories();
        //System.out.println("Categories inserted");
        //insertBusiness();
        //System.out.println("business inserted");
        //insertUsers();
       // System.out.println("users inserted");
        insertReviews();
        return i;
    }

    private void insertDays() throws SQLException {
        Statement stmt = connect.createStatement();
        stmt.execute("insert into days (day_id,day) values (1,'Sunday')");
        stmt.execute("insert into days (day_id,day) values (2,'Monday')");
        stmt.execute("insert into days (day_id,day) values (3,'Tuesday')");
        stmt.execute("insert into days (day_id,day) values (4,'Wednesday')");
        stmt.execute("insert into days (day_id,day) values (5,'Thursday')");
        stmt.execute("insert into days (day_id,day) values (6,'Friday')");
        stmt.execute("insert into days (day_id,day) values (7,'Saturday')");
        stmt.close();

    }

    private void insertParentCategories() throws SQLException {
        Statement stmt = connect.createStatement();
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (1,'Active Life', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (2,'Arts & Entertainment', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (3,'Automotive', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (4,'Car Rental', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (5,'Cafes', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (6,'Beauty & Spas', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (7,'Convenience Stores', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (8,'Dentists', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (9,'Doctors', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (10,'Drugstores', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (11,'Department Stores', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (12,'Education', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (13,'Event Planning & Services', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (14,'Flowers & Gifts', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (15,'Food', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (16,'Health & Medical', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (17,'Home Services', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (18,'Home & Garden', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (19,'Hospitals', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (20,'Hotels & Travel', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (21,'Hardware Stores', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (22,'Grocery', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (23,'Medical Centers', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (24,'Nurseries & Gardening', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (25,'Nightlife', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (26,'Restaurants', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (27,'Shopping', NULL)");
        stmt.execute("insert into categories (cat_id, cat_name, parent_id) values (28,'Transportation', NULL)");
        stmt.close();
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
                try (PreparedStatement preparedStatement1 = connect
                        .prepareStatement("insert into business values (?, ?, ?, ?, ? , ?, ?, ?, ?, ?, ?, default)")) {// Parameters start with 1
                    preparedStatement1.setString(1, obj.getString("business_id"));
                    preparedStatement1.setString(2, obj.getString("full_address"));
                    preparedStatement1.setBoolean(3, obj.getBoolean("open"));
                    preparedStatement1.setString(4, obj.getString("city"));
                    preparedStatement1.setString(5, obj.getString("state"));
                    preparedStatement1.setDouble(6, obj.getDouble("latitude"));
                    preparedStatement1.setDouble(7, obj.getDouble("longitude"));
                    preparedStatement1.setInt(8, obj.getInt("review_count"));
                    preparedStatement1.setString(9, obj.getString("name"));
                    preparedStatement1.setString(10, obj.getJSONArray("neighborhoods").toString());
                    preparedStatement1.setFloat(11, (float) obj.getDouble("stars"));
                    preparedStatement1.executeUpdate();
                }

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

                        try (PreparedStatement preparedStatement1 = connect.prepareStatement("insert /*+ ignore_row_on_dupkey_index(category_business_link, id) */ into category_business_link (id,business_id, cat_id) values (?,?,?)")) {
                            preparedStatement1.setInt(1, b);
                            preparedStatement1.setString(2, obj.getString("business_id"));
                            preparedStatement1.setInt(3, parentCatList.get(b));
                            preparedStatement1.executeUpdate();
                        }

                        List<String> cList = new ArrayList<String>();
                        Collections.addAll(cList, cArray);
                        cList.remove(rmList.get(b));

                        for (int i = 0; i < cList.size(); i++) {

                            try (PreparedStatement preparedStatement1 = connect.prepareStatement("select cat_id from categories where cat_name = ? AND parent_id = ?")) {
                                preparedStatement1.setString(1, cList.get(i));
                                preparedStatement1.setInt(2, parentCatList.get(b));
                                resultSet = preparedStatement1.executeQuery();

                                if (resultSet.isFirst()) {

                                } else {

                                    try (PreparedStatement preparedStatement2 = connect.prepareStatement("insert /*+ ignore_row_on_dupkey_index(categories, cat_id) */ into categories (cat_id,cat_name, parent_id) values (?,?,?)")) {
                                        preparedStatement2.setInt(1, i);
                                        preparedStatement2.setString(2, cList.get(i));
                                        preparedStatement2.setInt(3, parentCatList.get(b));
                                        preparedStatement2.executeUpdate();
                                    }

                                }
                                resultSet.close();
                            }

                            try (PreparedStatement preparedStatement1 = connect.prepareStatement("select cat_id from categories where cat_name = ? AND parent_id = ?")) {
                                preparedStatement1.setString(1, cList.get(i));
                                preparedStatement1.setInt(2, parentCatList.get(b));
                                resultSet = preparedStatement1.executeQuery();

                                if (!resultSet.next()) {

                                } else {

                                    int idd = resultSet.getInt(1);
                                    try (PreparedStatement preparedStatement2 = connect.prepareStatement("insert /*+ ignore_row_on_dupkey_index(business_id, cat_id) */ into category_business_link (id,business_id, cat_id) values (?,?,?)")) {
                                        preparedStatement2.setInt(1, i);
                                        preparedStatement2.setString(2, obj.getString("business_id"));
                                        preparedStatement2.setInt(3, idd);
                                        preparedStatement2.executeUpdate();
                                    }
                                }
                            }
                        }
                    }

                }

                JSONObject attrObj = obj.getJSONObject("attributes");
                Iterator<String> keys = attrObj.keys();

                while (keys.hasNext()) {
                    int atr_id;
                    String key = keys.next();
                    if (attrObj.get(key) instanceof Boolean) {
                        try (PreparedStatement preparedStatement1 = connect.prepareStatement("select max(id) from attributes")) {
                            try (ResultSet resultSet1 = preparedStatement1.executeQuery()) {
                                resultSet1.next();
                                atr_id = resultSet1.getInt(1) + 1;
                            }
                            try (PreparedStatement preparedStatement2 = connect.prepareStatement("insert /*+ ignore_row_on_dupkey_index(attributes, id) */ into attributes (id, attribute) values (?,?)")) {
                                preparedStatement2.setInt(1, atr_id);
                                preparedStatement2.setString(2, key);
                                try {
                                    preparedStatement2.executeUpdate();
                                } catch (Exception e) {

                                }
                            }
                        }

                        if (attrObj.getBoolean(key)) {
                            int att_bus_id=0;
                            int idd=0;
                            try (PreparedStatement preparedStatement1 = connect.prepareStatement("select id from attributes where attribute = ? ")) {
                                preparedStatement1.setString(1, key);
                                try (ResultSet resultSet1 = preparedStatement1.executeQuery()) {

                                    if (!resultSet1.next()) {
                                    } else {
                                        idd = resultSet1.getInt(1);
                                        try (PreparedStatement preparedStatement2 = connect.prepareStatement("select max(id) from attribute_business_link")) {
                                            try (ResultSet resultSet2 = preparedStatement2.executeQuery()) {
                                                resultSet2.next();
                                                att_bus_id = resultSet2.getInt(1) + 1;
                                            }
                                        }

                                    }
                                }
                            }
                            try (PreparedStatement preparedStatement1 = connect.prepareStatement("insert into attribute_business_link (id, business_id, attr_id) values (?,?,?)")) {
                                preparedStatement1.setInt(1, att_bus_id);
                                preparedStatement1.setString(2, obj.getString("business_id"));
                                preparedStatement1.setInt(3, idd);
                                try
                                {
                                    preparedStatement1.executeUpdate();
                                }
                                catch (Exception e)
                                {
                                    
                                }
                            }
                        }

                    } else if (attrObj.get(key) instanceof JSONObject) {

                        int idd=0;
                        JSONObject subObject = attrObj.getJSONObject(key);
                        Iterator<String> subKeys = subObject.keys();
                        while (subKeys.hasNext()) {
                            String skey = subKeys.next();
                            try (PreparedStatement preparedStatement1 = connect.prepareStatement("select max(id) from attributes")) {
                                try (ResultSet resultSet1 = preparedStatement1.executeQuery()) {
                                    resultSet1.next();
                                    atr_id = resultSet1.getInt(1) + 1;
                                }
                            }
                            try (PreparedStatement preparedStatement1 = connect.prepareStatement("insert /*+ ignore_row_on_dupkey_index(attributes, id) */ into attributes (id, attribute) values (?,?)")) {
                                preparedStatement1.setInt(1, atr_id);
                                preparedStatement1.setString(2, key + "_" + skey);
                                try {
                                    preparedStatement1.executeUpdate();
                                } catch (Exception e) {
                                }
                            }
                            if (subObject.getBoolean(skey)) {

                                try (PreparedStatement preparedStatement1 = connect.prepareStatement("select id from attributes where attribute = ? ")) {
                                    preparedStatement1.setString(1, key + "_" + skey);
                                    try (ResultSet resultSet1 = preparedStatement1.executeQuery()) {

                                        if (!resultSet1.next()) {
                                        } else {
                                            idd = resultSet1.getInt(1);
                                        }
                                    }
                                }
                                int att_bus_id;;
                                try (PreparedStatement preparedStatement1 = connect.prepareStatement("select max(id) from attribute_business_link")) {

                                    try (ResultSet resultSet1 = preparedStatement1.executeQuery()) {
                                        resultSet1.next();
                                        att_bus_id = resultSet1.getInt(1) + 1;
                                    }
                                }
                                try (PreparedStatement preparedStatement1 = connect.prepareStatement("insert into attribute_business_link (id,business_id, attr_id) values (?,?,?)")) {
                                    preparedStatement1.setInt(1, att_bus_id);
                                    preparedStatement1.setString(2, obj.getString("business_id"));
                                    preparedStatement1.setInt(3, idd);
                                    try{
                                        preparedStatement1.executeUpdate();
                                    }
                                    catch (Exception e)
                                    {
                                        
                                    }
                                }

                            }
                        }
                    } else if (attrObj.get(key) instanceof Integer) {
                        int xyz = attrObj.getInt(key);
                        try (PreparedStatement preparedStatement1 = connect.prepareStatement("select max(id) from attributes")) {
                            try (ResultSet resultSet1 = preparedStatement1.executeQuery()) {
                                resultSet1.next();
                                atr_id = resultSet1.getInt(1) + 1;
                            }
                        }
                        try (PreparedStatement preparedStatement1 = connect.prepareStatement("insert /*+ ignore_row_on_dupkey_index(attributes, id) */ into attributes (id, attribute) values (?,?)")) {
                            preparedStatement1.setInt(1, atr_id);
                            preparedStatement1.setString(2, key + "_" + xyz);

                            try {
                                preparedStatement1.executeUpdate();
                                preparedStatement1.close();
                            } catch (Exception e) {

                            }
                        }
                        int idd;
                        try (PreparedStatement preparedStatement1 = connect.prepareStatement("select id from attributes where attribute = ? ")) {
                            preparedStatement1.setString(1, key + "_" + xyz);
                            try (ResultSet resultSet1 = preparedStatement1.executeQuery()) {
                                if (!resultSet1.next()) {
                                } else {
                                    idd = resultSet1.getInt(1);
                                    try (PreparedStatement preparedStatement2 = connect.prepareStatement("insert into attribute_business_link (business_id, attr_id) values (?,?)")) {
                                        preparedStatement2.setString(1, obj.getString("business_id"));
                                        preparedStatement2.setInt(2, idd);
                                        preparedStatement2.executeUpdate();
                                    }
                                }
                            }
                        }
                    } else {
                        int idd;
                        String xyz = attrObj.getString(key);
                        try (PreparedStatement preparedStatement1 = connect.prepareStatement("select max(id) from attributes")) {
                            try (ResultSet resultSet1 = preparedStatement1.executeQuery()) {
                                resultSet1.next();
                                atr_id = resultSet1.getInt(1) + 1;
                            }
                            try (PreparedStatement preparedStatement2 = connect.prepareStatement("insert /*+ ignore_row_on_dupkey_index(attributes, id) */  into attributes (id, attribute) values (?,?)")) {
                                preparedStatement2.setInt(1, atr_id);
                                preparedStatement2.setString(2, key + "_" + xyz);
                                try {
                                    preparedStatement2.executeUpdate();
                                } catch (Exception e) {
                                }
                            }
                        }
                        try (PreparedStatement preparedStatement1 = connect.prepareStatement("select id from attributes where attribute = ? ")) {
                            preparedStatement1.setString(1, key + "_" + xyz);
                            try (ResultSet resultSet1 = preparedStatement1.executeQuery()) {
                                if (!resultSet1.next()) {
                                } else {
                                    int atr_bus_id;
                                    idd = resultSet1.getInt(1);
                                    try (PreparedStatement preparedStatement2 = connect.prepareStatement("select max(id) from attribute_business_link")) {
                                        try (ResultSet resultSet2 = preparedStatement2.executeQuery()) {
                                            resultSet2.next();
                                            atr_bus_id = resultSet2.getInt(1) + 1;
                                        }
                                    }

                                    try (PreparedStatement preparedStatement3 = connect.prepareStatement("insert into attribute_business_link (id,business_id, attr_id) values (?,?,?)")) {
                                        preparedStatement3.setInt(1, atr_bus_id);
                                        preparedStatement3.setString(2, obj.getString("business_id"));
                                        preparedStatement3.setInt(3, idd);
                                        preparedStatement3.executeUpdate();
                                    }
                                }
                            }
                        }

                    }

                    JSONObject hoursObj = obj.getJSONObject("hours");
                    Iterator<String> hKeys = hoursObj.keys();
                    int idd;
                    while (hKeys.hasNext()) {
                        String day = hKeys.next();
                        JSONObject dObject = hoursObj.getJSONObject(day);
                        String open = dObject.getString("open");
                        String close = dObject.getString("close");
                        Date oDate = format.parse(open);
                        Time oTime = new Time(oDate.getTime());
                        Date cDate = format.parse(close);
                        Time cTime = new Time(cDate.getTime());
                        try (PreparedStatement preparedStatement1 = connect.prepareStatement("select day_id from days where day = ? ")) {
                            preparedStatement1.setString(1, day);
                            try (ResultSet resultSet1 = preparedStatement1.executeQuery()) {
                                resultSet1.next();
                                idd = resultSet1.getInt(1);
                            }
                        }
                        int hour_id;
                        try (PreparedStatement preparedStatement1 = connect.prepareStatement("select max(id) from hours")) {
                            try (ResultSet resultSet1 = preparedStatement1.executeQuery()) {
                                resultSet1.next();
                                hour_id = resultSet1.getInt(1);
                            }
                        }
                        try (PreparedStatement preparedStatement1 = connect.prepareStatement("insert into hours(id,business_id,day_id,open,close) values (?,?,?,?,?)")) {
                            preparedStatement1.setInt(1, hour_id);
                            preparedStatement1.setString(2, obj.getString("business_id"));
                            preparedStatement1.setInt(3, idd);
                            preparedStatement1.setTime(4, oTime);
                            preparedStatement1.setTime(5, cTime);
                            preparedStatement1.executeUpdate();
                        }

                    }

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
                try (PreparedStatement preparedStatement1 = connect
                        .prepareStatement("insert into users values (?, ?, ?, ?, ? , ?, ?, ?, default)")) {
                    // Parameters start with 1
                    preparedStatement1.setString(1, obj.getString("user_id"));
                    preparedStatement1.setInt(2, votesObj.getInt("funny"));
                    preparedStatement1.setInt(3, votesObj.getInt("useful"));
                    preparedStatement1.setInt(4, votesObj.getInt("cool"));
                    preparedStatement1.setInt(5, obj.getInt("review_count"));
                    preparedStatement1.setString(6, obj.getString("name"));
                    preparedStatement1.setInt(7, obj.getInt("fans"));
                    preparedStatement1.setDouble(8, obj.getDouble("average_stars"));
                    preparedStatement1.executeUpdate();
                }

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
            int i = 0;
            while ((line = br.readLine()) != null) {
                JSONObject obj = new JSONObject(line);
                JSONObject votesObj = obj.getJSONObject("votes");
                try (PreparedStatement preparedStatement1 = connect
                        .prepareStatement("insert into reviews values (?, ?, ?, ?, ? , ?, ?, ? ,?, default)")) {
                    // Parameters start with 1
                    preparedStatement1.setString(1, obj.getString("review_id"));
                    preparedStatement1.setString(2, obj.getString("user_id"));
                    preparedStatement1.setString(3, obj.getString("business_id"));
                    preparedStatement1.setInt(5, votesObj.getInt("funny"));
                    preparedStatement1.setInt(4, votesObj.getInt("useful"));
                    preparedStatement1.setInt(6, votesObj.getInt("cool"));
                    preparedStatement1.setInt(7, obj.getInt("stars"));
                    preparedStatement1.setDate(8, new java.sql.Date(sdf.parse(obj.getString("date")).getTime()));
                    preparedStatement1.setString(9, obj.getString("text"));
                    try {
                        preparedStatement1.executeUpdate();
                        System.out.println("Review Saved");
                    } catch (Exception e) {
                        System.out.println(i);
                        i++;
                    }
                }
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
