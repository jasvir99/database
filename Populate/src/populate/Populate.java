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
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONObject;

/**
 *
 * @author damandeep
 */
public class Populate {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
        try {
            
            BufferedReader br = null;
            
            MySQLAccess ms = new MySQLAccess();
            ms.connect();
            ms.cleanTables();
            
            
        } catch (Exception ex) {
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
