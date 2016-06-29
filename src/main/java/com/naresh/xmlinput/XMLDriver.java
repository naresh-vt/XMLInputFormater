package com.naresh.xmlinput;
 
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 
public class XMLDriver {
 
    /** Naresh - for processing XML file using Hadoop MapReduce
     * @param args
     */
    public static void main(String[] args) {
        try {
            System.out.println("Started!!!");
            Configuration conf = new Configuration();
            
            String[] arg = new GenericOptionsParser(conf, args).getRemainingArgs();
 
            conf.set("START_TAG_KEY", "<employee>");
            conf.set("END_TAG_KEY", "</employee>");
 
            Job job = new Job(conf, "XML Processing Processing");
            job.setJarByClass(XMLDriver.class);
            job.setMapperClass(MyMapper.class);
 
            job.setNumReduceTasks(0);
 
            job.setInputFormatClass(XMLInputFormat.class);
            // job.setOutputValueClass(TextOutputFormat.class);
 
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);
 
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
 
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
            job.waitForCompletion(true);
            System.out.println("ENded");
 
        } catch (Exception e) {
            System.out.println("in Error");
            e.printStackTrace();
            System.out.println(e.getMessage().toString());
        }
 
    }
 
}
