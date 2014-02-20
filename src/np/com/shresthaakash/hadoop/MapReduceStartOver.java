package np.com.shresthaakash.hadoop;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
/**
 * @website 
 * @author siranami
 */
public class MapReduceStartOver {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            int res = ToolRunner.run(new Configuration(), new Task(), args);
            System.exit(res);
        } catch (Exception ex) {
            Logger.getLogger(MapReduceStartOver.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
