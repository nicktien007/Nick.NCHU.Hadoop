import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountSorting {


    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                context.write(new Text(token), new IntWritable(1));
            }
        }
    }


    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }




    public static class MapSort extends Mapper<LongWritable, Text, IntWritable,Text > {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            String token1 = tokenizer.nextToken();
            String token2 = tokenizer.nextToken();

            context.write(new IntWritable(Integer.parseInt(token2)),new Text(token1));
        }
    }


    public static class ReduceSort extends Reducer<IntWritable, Text,IntWritable, Text > {

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text v : values){
                context.write(key, v);
            }
        }
    }



    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        //Job Configuration
        Job job = Job.getInstance(conf, "wordcount");

        //set output key and value type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //set Map and Reduce Function
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setJarByClass(WordCountSorting.class);

        //set input and output Format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //set input and output Locations
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);


        Configuration conf2 = new Configuration();

        //Job Configuration
        Job job2 = Job.getInstance(conf2, "wordcountSort");

        //set output key and value type
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        //set Map and Reduce Function
        job2.setMapperClass(MapSort.class);
        job2.setReducerClass(ReduceSort.class);
        job2.setJarByClass(WordCountSorting.class);

        //set input and output Format
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        //set input and output Locations
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);

    }

}
