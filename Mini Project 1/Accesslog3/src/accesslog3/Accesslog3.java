/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package accesslog3;

import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.text.ParseException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class Accesslog3 {
    
public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    
    //private IntWritable hour = new IntWritable();
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
    
         private static Pattern logPattern = Pattern
         /*.compile("([^ ]*) ([^ ]*) ([^ ]*) \\[([^]]*)\\]"
                 + " \"([^\"]*)\""
                 + " ([^ ]*) ([^ ]*).*");*/
        .compile("%h %l %u %t \"%r\" %>s %b");
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          
        //    StringTokenizer itr = new StringTokenizer(value.toString());
      //while (itr.hasMoreTokens()) {
          String line = ((Text) value).toString();
     Matcher matcher = logPattern.matcher(line);
     if (matcher.matches()) {
         String item = matcher.group(5);
         String path = item.split(" ")[1];
               word.set(path);
                context.write(word, one);
         

      }
    }
  }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

       // private IntWritable result = new IntWritable();
       static int maximum=0;
       Text pathmax = new Text();
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context
        ) throws IOException, InterruptedException {
            
            
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if(maximum<sum){
              maximum=sum;
              pathmax.set(key);
            }
            //result.set(sum);
            //context.write(key, result);
        }
        public void cleanup(Context context) throws IOException, InterruptedException{
            context.write(pathmax, new IntWritable(maximum) );
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Job job = Job.getInstance(conf, "Accesslog3");
        job.setJarByClass(Accesslog3.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
