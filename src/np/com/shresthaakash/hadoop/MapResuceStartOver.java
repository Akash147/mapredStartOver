package np.com.shresthaakash.hadoop;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
/**
 *
 * @author siranami
 */
public class MapResuceStartOver {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            int res = ToolRunner.run(new Configuration(), new Task(), args);
            System.exit(res);
        } catch (Exception ex) {
            Logger.getLogger(MapResuceStartOver.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
