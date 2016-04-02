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
            
            try {
                // TODO code application logic here
                
                String fileName = "file:/home/jass/Downloads/database/dataset/yelp_business.json";
                URI fileUri = new URI(fileName);
                br = new BufferedReader(new FileReader(new File(fileUri)));
                String line;
                int i =1;
                int big = 0;
                int lineNumber = -1;
                while((line = br.readLine())!=null) {
//                    JSONObject obj = new JSONObject(line);
//                    Iterator<?> keys = obj.keys();
//                    System.out.print("Reading Obj "+i+"\nKeys:\t");
//                    while(keys.hasNext()) {
//                        System.out.print(keys.next()+"\t");
//                    }
//                    if(big<line.length())
//                    {
//                        big = line.length();
//                        lineNumber = i;
//                    }
//                    System.out.print("\n");
//                    i++;
                }
                System.out.println("Biggest Line is "+lineNumber+" with "+big+" characters");
                
            } catch (FileNotFoundException ex) {
                Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
            } catch (URISyntaxException ex) {
                Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (Exception ex) {
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
