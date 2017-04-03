
package accesslog1;

import java.io.IOException;
/*import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.mapreduce.Mapper;*/

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



public class Accesslog1{
    
public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    
   
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
    //String logp = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+)";
         private static Pattern logPattern = Pattern//.compile(logp);//private static 
         //.compile("([^ ]*) ([^ ]*) ([^ ]*) \\[([^]]*)\\]"
         //        + " \"([^\"]*)\""
         //        + " ([^ ]*) ([^ ]*).*");
        .compile("%h %l %u %t \"%r\" %>s %b");
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         
          String line = value.toString();
     Matcher matcher = logPattern.matcher(line);
     if (matcher.matches()) {
         String item = matcher.group(5);
         String path = item.split(" ")[1];
         if(path.equals("/assets/img/home-logo.png")){
               word.set(path);
                context.write(word, one);
         }

      }
    }
  }


    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Job job = Job.getInstance(conf, "Accesslog1");
        job.setJarByClass(Accesslog1.class);
        
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
