/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import static java.util.Collections.list;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.table.DefaultTableModel;

/**
 *
 * @author gurjot
 */
public class MainJFrame extends javax.swing.JFrame {

    private Connection connect = null;
    private Statement statement = null;
    private PreparedStatement preparedStatement = null;
    private ResultSet resultSet = null;

    private int parentId = 0;

    private List<Integer> catsIDs = new ArrayList<>();
    private List<Integer> attrsIDs = new ArrayList<>();

    /**
     * Creates new form MainJFrame
     */
    public MainJFrame() {
        initComponents();
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        jDialog1 = new javax.swing.JDialog();
        jButton1 = new javax.swing.JButton();
        jScrollPane1 = new javax.swing.JScrollPane();
        jList1 = new javax.swing.JList<>();
        jScrollPane2 = new javax.swing.JScrollPane();
        jList2 = new javax.swing.JList<>();
        jScrollPane3 = new javax.swing.JScrollPane();
        jList3 = new javax.swing.JList<>();
        jScrollPane4 = new javax.swing.JScrollPane();
        jTable1 = new javax.swing.JTable();
        jComboBox1 = new javax.swing.JComboBox<>();
        jComboBox2 = new javax.swing.JComboBox<>();
        jComboBox3 = new javax.swing.JComboBox<>();
        jComboBox4 = new javax.swing.JComboBox<>();
        jLabel1 = new javax.swing.JLabel();
        jLabel2 = new javax.swing.JLabel();
        jLabel3 = new javax.swing.JLabel();
        jLabel4 = new javax.swing.JLabel();
        jButton2 = new javax.swing.JButton();
        jButton3 = new javax.swing.JButton();

        javax.swing.GroupLayout jDialog1Layout = new javax.swing.GroupLayout(jDialog1.getContentPane());
        jDialog1.getContentPane().setLayout(jDialog1Layout);
        jDialog1Layout.setHorizontalGroup(
            jDialog1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 400, Short.MAX_VALUE)
        );
        jDialog1Layout.setVerticalGroup(
            jDialog1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 300, Short.MAX_VALUE)
        );

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

        jButton1.setText("Start");
        jButton1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton1ActionPerformed(evt);
            }
        });

        jList1.setSelectionMode(javax.swing.ListSelectionModel.SINGLE_SELECTION);
        jList1.addListSelectionListener(new javax.swing.event.ListSelectionListener() {
            public void valueChanged(javax.swing.event.ListSelectionEvent evt) {
                jList1ValueChanged(evt);
            }
        });
        jScrollPane1.setViewportView(jList1);

        jList2.addListSelectionListener(new javax.swing.event.ListSelectionListener() {
            public void valueChanged(javax.swing.event.ListSelectionEvent evt) {
                jList2ValueChanged(evt);
            }
        });
        jScrollPane2.setViewportView(jList2);

        jList3.addListSelectionListener(new javax.swing.event.ListSelectionListener() {
            public void valueChanged(javax.swing.event.ListSelectionEvent evt) {
                jList3ValueChanged(evt);
            }
        });
        jScrollPane3.setViewportView(jList3);

        jTable1.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null},
                {null, null, null, null, null}
            },
            new String [] {
                "Business", "City", "State", "Stars", "bid"
            }
        ) {
            boolean[] canEdit = new boolean [] {
                false, false, false, false, false
            };

            public boolean isCellEditable(int rowIndex, int columnIndex) {
                return canEdit [columnIndex];
            }
        });
        jTable1.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                jTable1MouseClicked(evt);
            }
        });
        jScrollPane4.setViewportView(jTable1);
        if (jTable1.getColumnModel().getColumnCount() > 0) {
            jTable1.getColumnModel().getColumn(0).setPreferredWidth(350);
            jTable1.getColumnModel().getColumn(1).setPreferredWidth(200);
            jTable1.getColumnModel().getColumn(2).setPreferredWidth(100);
            jTable1.getColumnModel().getColumn(3).setPreferredWidth(100);
        }

        jComboBox1.setModel(new javax.swing.DefaultComboBoxModel<>(new String[] { "All" }));
        jComboBox1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jComboBox1ActionPerformed(evt);
            }
        });

        jComboBox2.setModel(new javax.swing.DefaultComboBoxModel<>(new String[] { "Any", "00:00", "01:00", "02:00", "03:00", "04:00", "05:00", "06:00", "07:00", "08:00", "09:00", "10:00", "11:00", "12:00", "13:00", "14:00", "15:00", "16:00", "17:00", "18:00", "19:00", "20:00", "21:00", "22:00", "23:00" }));

        jComboBox3.setModel(new javax.swing.DefaultComboBoxModel<>(new String[] { "Any", "00:00", "01:00", "02:00", "03:00", "04:00", "05:00", "06:00", "07:00", "08:00", "09:00", "10:00", "11:00", "12:00", "13:00", "14:00", "15:00", "16:00", "17:00", "18:00", "19:00", "20:00", "21:00", "22:00", "23:00" }));

        jComboBox4.setModel(new javax.swing.DefaultComboBoxModel<>(new String[] { "All", "Any" }));

        jLabel1.setText("Day of the week");

        jLabel2.setText("From");

        jLabel3.setText("To");

        jLabel4.setText("Search for");

        jButton2.setText("Search");
        jButton2.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                jButton2MouseClicked(evt);
            }
        });
        jButton2.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton2ActionPerformed(evt);
            }
        });

        jButton3.setText("Close");
        jButton3.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton3ActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jButton1, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                    .addGroup(layout.createSequentialGroup()
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(jComboBox1, javax.swing.GroupLayout.PREFERRED_SIZE, 139, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jLabel1))
                        .addGap(18, 18, 18)
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(jComboBox2, javax.swing.GroupLayout.PREFERRED_SIZE, 125, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jLabel2))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(jComboBox3, javax.swing.GroupLayout.PREFERRED_SIZE, 114, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jLabel3))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(jLabel4)
                            .addGroup(layout.createSequentialGroup()
                                .addComponent(jComboBox4, javax.swing.GroupLayout.PREFERRED_SIZE, 151, javax.swing.GroupLayout.PREFERRED_SIZE)
                                .addGap(212, 212, 212)
                                .addComponent(jButton2, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)))
                        .addGap(18, 18, 18)
                        .addComponent(jButton3, javax.swing.GroupLayout.PREFERRED_SIZE, 124, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(layout.createSequentialGroup()
                        .addComponent(jScrollPane1, javax.swing.GroupLayout.PREFERRED_SIZE, 189, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(jScrollPane2, javax.swing.GroupLayout.PREFERRED_SIZE, 199, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(jScrollPane3, javax.swing.GroupLayout.PREFERRED_SIZE, 188, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(jScrollPane4, javax.swing.GroupLayout.PREFERRED_SIZE, 429, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addGap(0, 30, Short.MAX_VALUE)))
                .addContainerGap())
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jButton1)
                .addGap(18, 18, 18)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING, false)
                    .addComponent(jScrollPane2, javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jScrollPane3, javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jScrollPane4, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE, 548, Short.MAX_VALUE)
                    .addComponent(jScrollPane1))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, 22, Short.MAX_VALUE)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel1)
                    .addComponent(jLabel2)
                    .addComponent(jLabel3)
                    .addComponent(jLabel4))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jComboBox1, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jComboBox2, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jComboBox3, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jComboBox4, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jButton2, javax.swing.GroupLayout.PREFERRED_SIZE, 33, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jButton3, javax.swing.GroupLayout.PREFERRED_SIZE, 33, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(35, 35, 35))
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void jButton1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton1ActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jButton1ActionPerformed

    private void jButton2ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton2ActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jButton2ActionPerformed

    private void jComboBox1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jComboBox1ActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jComboBox1ActionPerformed

    private void jButton3ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton3ActionPerformed
        // TODO add your handling code here:
        System.exit(0);
    }//GEN-LAST:event_jButton3ActionPerformed

    private void jList1ValueChanged(javax.swing.event.ListSelectionEvent evt) {//GEN-FIRST:event_jList1ValueChanged
        try {
            // TODO add your handling code here:
            String a = jList1.getSelectedValue();
            preparedStatement = connect.prepareStatement("SELECT cat_id FROM categories WHERE cat_name = ?");
            preparedStatement.setString(1, a);
            System.out.println(a);
            resultSet = preparedStatement.executeQuery();
            resultSet.next();
            parentId = resultSet.getInt(1);
            preparedStatement = connect.prepareStatement("SELECT * FROM categories WHERE parent_id = ?");
            preparedStatement.setInt(1, parentId);
            resultSet = preparedStatement.executeQuery();

            Vector<String> v = new Vector<>();
            while (resultSet.next()) {
                v.add(resultSet.getString(2));
            }
            jList2.setListData(v);
        } catch (SQLException ex) {
            Logger.getLogger(MainJFrame.class.getName()).log(Level.SEVERE, null, ex);
        }
    }//GEN-LAST:event_jList1ValueChanged

    private void jList2ValueChanged(javax.swing.event.ListSelectionEvent evt) {//GEN-FIRST:event_jList2ValueChanged
        // TODO add your handling code here:
        List<Integer> catIds = new ArrayList<>();
        List<String> l = jList2.getSelectedValuesList();
        System.out.println("Selected Sub Cats: " + l.size());
        for (int i = 0; i < l.size(); i++) {
            try {
                String x = l.get(i);
                preparedStatement = connect.prepareStatement("SELECT cat_id FROM categories WHERE cat_name = ? AND parent_id = ?");
                preparedStatement.setString(1, x);
                preparedStatement.setInt(2, parentId);
                resultSet = preparedStatement.executeQuery();
                resultSet.next();
                catIds.add(resultSet.getInt(1));
            } catch (SQLException ex) {
                Logger.getLogger(MainJFrame.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        catsIDs = catIds;
        System.out.println("Selected Sub Cats Ids: " + catIds.size());
        List<String> bIds = new ArrayList<>();
        for (int i = 0; i < catIds.size(); i++) {
            try {
                preparedStatement = connect.prepareStatement("SELECT business_id FROM category_business_link WHERE cat_id = ?");

                preparedStatement.setInt(1, catIds.get(i));
                resultSet = preparedStatement.executeQuery();
                resultSet.next();
                while (resultSet.next()) {

                    bIds.add(resultSet.getString(1));
                }

            } catch (SQLException ex) {
                Logger.getLogger(MainJFrame.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        System.out.println("Selected BIDs: " + bIds.size());
        List<Integer> attrIds = new ArrayList<>();

        for (int i = 0; i < bIds.size(); i++) {
            try {
                preparedStatement = connect.prepareStatement("SELECT attr_id FROM attribute_business_link WHERE business_id = ?");

                preparedStatement.setString(1, bIds.get(i));
                resultSet = preparedStatement.executeQuery();
                resultSet.next();
                while (resultSet.next()) {

                    attrIds.add(resultSet.getInt(1));
                }

            } catch (SQLException ex) {
                Logger.getLogger(MainJFrame.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        System.out.println("Selected Attr Ids : " + attrIds.size());
        attrIds = new ArrayList<Integer>(new LinkedHashSet<Integer>(attrIds));
        attrsIDs = attrIds;
        System.out.println("Attr Ids : " + attrIds.size());
        Vector<String> data = new Vector<>();
        for (int i = 0; i < attrIds.size(); i++) {
            try {
                preparedStatement = connect.prepareStatement("SELECT attribute FROM attributes WHERE id = ?");
                preparedStatement.setInt(1, attrIds.get(i));
                System.out.println(preparedStatement.toString());
                resultSet = preparedStatement.executeQuery();
                resultSet.next();

                data.add(resultSet.getString(1));

            } catch (SQLException ex) {
                Logger.getLogger(MainJFrame.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        jList3.setListData(data);
        System.out.println("Data: " + data.size());
    }//GEN-LAST:event_jList2ValueChanged

    private void jList3ValueChanged(javax.swing.event.ListSelectionEvent evt) {//GEN-FIRST:event_jList3ValueChanged
        try {
            // TODO add your handling code here:
            int[] cPOS = jList2.getSelectedIndices();
            List<Integer> selCa = new ArrayList<>();
            for (int i = 0; i < cPOS.length; i++) {
                selCa.add(catsIDs.get(i));
            }
            int[] aPOS = jList3.getSelectedIndices();
            List<Integer> selAt = new ArrayList<>();
            for (int i = 0; i < cPOS.length; i++) {
                selAt.add(attrsIDs.get(i));
            }

            List<String> bIds1 = new ArrayList<>();
            List<String> bIds2 = new ArrayList<>();

            for (int i = 0; i < selCa.size(); i++) {
                preparedStatement = connect.prepareStatement("SELECT business_id FROM category_business_link WHERE cat_id = ?");

                preparedStatement.setInt(1, selCa.get(i));
                resultSet = preparedStatement.executeQuery();
                resultSet.next();
                while (resultSet.next()) {

                    bIds1.add(resultSet.getString(1));
                }
            }
            System.out.println("BID1: " + bIds1.size());

            for (int i = 0; i < selAt.size(); i++) {
                preparedStatement = connect.prepareStatement("SELECT business_id FROM attribute_business_link WHERE attr_id = ?");

                preparedStatement.setInt(1, selAt.get(i));
                resultSet = preparedStatement.executeQuery();
                resultSet.next();
                while (resultSet.next()) {

                    bIds2.add(resultSet.getString(1));
                }
            }
            System.out.println("BID2: " + bIds2.size());
            bIds1.retainAll(bIds2);
            System.out.println("BID1: " + bIds1.size());
            show_business(bIds1);

        } catch (SQLException ex) {
            Logger.getLogger(MainJFrame.class.getName()).log(Level.SEVERE, null, ex);
        }

    }//GEN-LAST:event_jList3ValueChanged

    private void jTable1MouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_jTable1MouseClicked
        // TODO add your handling code here:

        javax.swing.JTable jTable = (javax.swing.JTable) evt.getSource();
        int rowSelected = jTable.getSelectedRow();

        String businessId = jTable.getModel().getValueAt(rowSelected, 4).toString();

        UserReviews review = new UserReviews();
        review.initializeUserReviewsWindow();
        review.show_reviews(businessId);


    }//GEN-LAST:event_jTable1MouseClicked

    private void jButton2MouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_jButton2MouseClicked
        // TODO add your handling code here:

        try {
            // TODO add your handling code here:

            int day_id = jComboBox1.getSelectedIndex();
            List<String> bIds1 = get_values_as_per_category_list();
            List<String> bIds2 = get_values_as_per_attribute_list();

            System.out.println("BID1: !" + bIds1.size());

            System.out.println("BID2: =" + bIds2.size());
            bIds1.retainAll(bIds2);
            System.out.println("BID1:+ " + bIds1.size());
            if (day_id > 0) {
                try {
                    List<String> bIds3 = new ArrayList<>();
                    preparedStatement = connect.prepareStatement("SELECT business_id FROM hours WHERE day_id = ?");
                    preparedStatement.setInt(1, day_id);
                    resultSet = preparedStatement.executeQuery();
                    resultSet.next();
                    while (resultSet.next()) {
                        bIds3.add(resultSet.getString(1));
                    }
                    System.out.println("BID3: #" + bIds3.size());

                    bIds1.retainAll(bIds3);
                    System.out.println("BID3: $" + bIds3.size());
                    System.out.println("BID1: %" + bIds1.size());
                } catch (Exception e) {
                }
            }

            try {
                String from_time;
                String to_time;
                if (jComboBox2.getSelectedIndex() == 0) {
                    from_time = "00:00:00";
                } else {
                    from_time = jComboBox2.getSelectedItem().toString();
                }

                if (jComboBox3.getSelectedIndex() == 0) {
                    to_time = "11:59:00";
                } else {
                    to_time = jComboBox3.getSelectedItem().toString();
                }
                System.out.println(from_time);
                System.out.println(to_time);
                preparedStatement = connect.prepareStatement("SELECT business_id FROM hours WHERE CAST(open AS time) >= ? AND CAST(close AS time) <= ?");
                preparedStatement.setString(1, from_time);
                preparedStatement.setString(2, to_time);
                resultSet = preparedStatement.executeQuery();
                resultSet.next();
                List<String> bIds4 = new ArrayList<>();
                while (resultSet.next()) {
                    bIds4.add(resultSet.getString(1));
                }

                bIds1.retainAll(bIds4);
            } catch (Exception e) {

            }
            show_business(bIds1);

        } catch (Exception ex) {
        }

    }//GEN-LAST:event_jButton2MouseClicked

    public List<String> get_values_as_per_category_list() {
        List<String> bIds1 = new ArrayList<>();
        try {

            int[] cPOS = jList2.getSelectedIndices();
            List<Integer> selCa = new ArrayList<>();
            for (int i = 0; i < cPOS.length; i++) {
                selCa.add(catsIDs.get(i));
            }

            for (int i = 0; i < selCa.size(); i++) {
                preparedStatement = connect.prepareStatement("SELECT business_id FROM category_business_link WHERE cat_id = ?");

                preparedStatement.setInt(1, selCa.get(i));
                resultSet = preparedStatement.executeQuery();
                resultSet.next();
                while (resultSet.next()) {

                    bIds1.add(resultSet.getString(1));
                }
            }

        } catch (SQLException ex) {
            Logger.getLogger(MainJFrame.class.getName()).log(Level.SEVERE, null, ex);
        }

        return bIds1;

    }

    public List<String> get_values_as_per_attribute_list() {
        List<String> bIds1 = new ArrayList<>();
        try {

            int[] aPOS = jList3.getSelectedIndices();
            int[] cPOS = jList2.getSelectedIndices();
            List<Integer> selAt = new ArrayList<>();
            for (int i = 0; i < cPOS.length; i++) {
                selAt.add(attrsIDs.get(i));
            }

            for (int i = 0; i < selAt.size(); i++) {
                preparedStatement = connect.prepareStatement("SELECT business_id FROM attribute_business_link WHERE attr_id = ?");

                preparedStatement.setInt(1, selAt.get(i));
                resultSet = preparedStatement.executeQuery();
                resultSet.next();
                while (resultSet.next()) {

                    bIds1.add(resultSet.getString(1));
                }
            }

        } catch (SQLException ex) {
            Logger.getLogger(MainJFrame.class.getName()).log(Level.SEVERE, null, ex);
        }

        return bIds1;

    }

    public static void deleteAllRows(final DefaultTableModel model) {
        for (int i = model.getRowCount() - 1; i >= 0; i--) {
            model.removeRow(i);
        }
    }

    public void connect() {
        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Class.forName("com.mysql.jdbc.Driver");
            // Setup the connection with the DB
            connect = DriverManager
                    .getConnection("jdbc:oracle:thin:@localhost:1521:sysdba", "c##jasvir", "password");

//            connect = DriverManager.getConnection("jdbc:oracle:thin:@hostname:port Number:databaseName","user","password");
            // Statements allow to issue SQL queries to the database
            statement = connect.createStatement();
        } catch (ClassNotFoundException | SQLException ex) {
            Logger.getLogger(MainJFrame.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            resultSet = statement.executeQuery("SELECT * FROM days");
            while (resultSet.next()) {
                jComboBox1.addItem(resultSet.getString(2));
            }
        } catch (SQLException ex) {
            Logger.getLogger(MainJFrame.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            resultSet = statement.executeQuery("SELECT * FROM categories WHERE parent_id IS NULL");

            Vector<String> v = new Vector<>();
            while (resultSet.next()) {
                v.add(resultSet.getString(2));
            }
            jList1.setListData(v);
        } catch (SQLException ex) {
            Logger.getLogger(MainJFrame.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void show_business(List<String> bIds) {
        DefaultTableModel model = (DefaultTableModel) jTable1.getModel();
        deleteAllRows(model);
        jTable1.setModel(model);
        System.out.println(bIds.size());
        try {
            for (int i = 0; i < bIds.size(); i++) {

                preparedStatement = connect.prepareStatement("Select name, full_address, city, state, stars, business_id from business where business_id = ?");
                preparedStatement.setString(1, bIds.get(i));
                resultSet = preparedStatement.executeQuery();
                resultSet.next();
                Object[] row = {resultSet.getString(1) + " " + resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6)};
                model.addRow(row);
            }
            jTable1.setModel(model);
            try {
                jTable1.removeColumn(jTable1.getColumnModel().getColumn(4));
            } catch (Exception e) {

            }
        } catch (SQLException ex) {
            Logger.getLogger(MainJFrame.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        /* Set the Nimbus look and feel */
        //<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
        /* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
         * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html 
         */

        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException ex) {
            java.util.logging.Logger.getLogger(MainJFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(MainJFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(MainJFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(MainJFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>

        /* Create and display the form */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                MainJFrame jFrame = new MainJFrame();
                jFrame.setVisible(true);
                jFrame.connect();

            }
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton jButton1;
    private javax.swing.JButton jButton2;
    private javax.swing.JButton jButton3;
    private javax.swing.JComboBox<String> jComboBox1;
    private javax.swing.JComboBox<String> jComboBox2;
    private javax.swing.JComboBox<String> jComboBox3;
    private javax.swing.JComboBox<String> jComboBox4;
    private javax.swing.JDialog jDialog1;
    private javax.swing.JLabel jLabel1;
    private javax.swing.JLabel jLabel2;
    private javax.swing.JLabel jLabel3;
    private javax.swing.JLabel jLabel4;
    private javax.swing.JList<String> jList1;
    private javax.swing.JList<String> jList2;
    private javax.swing.JList<String> jList3;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JScrollPane jScrollPane2;
    private javax.swing.JScrollPane jScrollPane3;
    private javax.swing.JScrollPane jScrollPane4;
    private javax.swing.JTable jTable1;
    // End of variables declaration//GEN-END:variables
}
