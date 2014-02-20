package np.com.shresthaakash.hadoop;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.io.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;

/**
 *
 * @author siranami
 */
public class Task extends Configured implements Tool {
    
    @Override
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), Task.class);
        conf.setJobName("My Task");

        // @TODO set the class of output key and value. These output types corresponds to what will be written to file after reducer completes.
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        List<String> other_args = new ArrayList<String>();
        for(int i=0; i < args.length; ++i) {
            try {
                if ("-m".equals(args[i])) {
                    conf.setNumMapTasks(Integer.parseInt(args[++i]));
                } else if ("-r".equals(args[i])) {
                    conf.setNumReduceTasks(Integer.parseInt(args[++i]));
                } else {
                    other_args.add(args[i]);
                }
            } catch (NumberFormatException exception) {
                System.out.println("Give int value instead of " + args[i]);
                exception.printStackTrace();
            } catch (ArrayIndexOutOfBoundsException exception) {
                System.out.println("Parameter missing " +  args[i-1]);
                exception.printStackTrace();
            }
        }

        if (other_args.size() != 2) {
            return 1;
        }
        FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
        FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

        JobClient.runJob(conf);
        return 0;
    }
    
    /**
     * Class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
     * Check Apache Documentation for this class.
     */
    public static class Map extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, DoubleWritable> {
        
        /**
         * This receives each line of your input file. Ex: comma separated line if your input file is csv.
         * Your code need to process on that line and produce output to OutputCollector.
         * The OutputCollector then forwards the collected output to REDUCER after shuffling wrt Key.class
         * @param key 
         * @param inputs is a line of your input file.
         * @param output is OutputCollector<KEYOUT,VALUEOUT>. You map your result as output.collect(keyVar, valueVar). Set the class as required.
         * @param reporter
         * @throws IOException 
         */
        public void map(LongWritable key, Text input,
                            OutputCollector<Text, DoubleWritable> output,
                            Reporter reporter) throws IOException {
        Text keyVar   = new Text();
        DoubleWritable valueVar = new DoubleWritable();
        
        // @TODO Process the input line
        // Ex: String s[] = input.split(',');
        // keyVar = s[1]; valueVar = s[2];
        
        output.collect(keyVar, valueVar);
        }
    }
    /**es corresponds to what will be wr
     * Class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
     * Check Apache Documentation for this class.
     * KEYIN, VALUEIN corresponds to what mapper has produced as KEYOUT,VALUEOUT.
     * KEYOUT, VALUEOUT of this class corresponds to what will be written to file. And this corresponds to something configured above in JobConf in run() module.
     */
    public static class Reduce extends MapReduceBase
            implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        /**
         * This receives its input from the shuffler(combiner). We are provided with a bundle of values, all having same keyVar after shuffling.
         * We iterate over values of one bundle at one time, process them and provide output to OutputCollector, which then writes to file.
         * @param key
         * @param values are those obtained after shuffling. We iterate over all the values with same keys.
         * @param output
         * @param reporter
         * @throws IOException 
         */
        public void reduce(Text key, Iterator<DoubleWritable> values,
                               OutputCollector<Text, DoubleWritable> output,
                               Reporter reporter) throws IOException {
            DoubleWritable a = new DoubleWritable();
            while(values.hasNext()){
                // @TODO process the bundle. { 'key': { value1, value2, value3 } };
            }
            output.collect(key, a);
        }
    }
    
}
