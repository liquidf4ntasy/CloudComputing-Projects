package edu.uta.cse6331;
/* CSE 6331 - Fall 17
 * Assignment 1 - Two-Step Matrix Multiplication using Hadoop MapReduce
 * Name: Satyajit Deshmukh
 * UTA ID: 100141772
 * 
 * References
 * https://www.youtube.com/watch?v=ap9UOMEMoLw&t=852s
 * https://adhoop.wordpress.com/2012/03/28/matrix_multiplication_2_step/
 * https://lendap.wordpress.com/2015/02/16/matrix-multiplication-with-mapreduce/
 * https://www.youtube.com/watch?v=4GApgxK7Jlc
 */

import java.io.IOException; 
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Multiply {
    public static class StepOneMapperM
	extends Mapper<LongWritable, Text, Text, Text> {
        // Mapper for entries of  matrix M
	private Text outKey = new Text();
	private Text outVal = new Text();
	public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
		String ipline = value.toString();
		String[] RowColumnIndex = ipline.split(",");
		outKey.set(RowColumnIndex[1]);
		outVal.set("M," + RowColumnIndex[0] + "," + RowColumnIndex[2]);
		context.write(outKey, outVal);
    }
    }
    
    public static class StepOneMapperN
	extends Mapper<LongWritable, Text, Text, Text> {
        // Mapper for entries of  matrix N
	private Text outKey = new Text();
	private Text outVal = new Text();
	public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
	    // fill in your code
		String ipline = value.toString();
		String[] indexVal = ipline.split(",");
		outKey.set(indexVal[0]);
		outVal.set("N," + indexVal[1] + "," + indexVal[2]);
		context.write(outKey, outVal);
		}
    }
    
    
    public static class StepOneReducer
	extends Reducer<Text, Text, Text, Text> {
	private Text outKey = new Text();
	private Text outVal = new Text();
	public void reduce(Text key, Iterable<Text> values, Context context)
	    throws IOException, InterruptedException {
		String[] val;
		ArrayList<Entry<Double, Double>> Entries_M = new ArrayList<Entry<Double, Double>>();   
		ArrayList<Entry<Double, Double>> Entries_N = new ArrayList<Entry<Double, Double>>();  
		for(Text v : values){
			// delimiter comma is used to split and tokenize
			val = v.toString().split(",");
			if(val[0].equals("M")){
				Entries_M.add(new SimpleEntry<Double, Double>(Double.parseDouble(val[1]),Double.parseDouble(val[2])));}   
				//(i, v) tuple is stored as keyvalue pair
			else{
				Entries_N.add(new SimpleEntry<Double, Double>(Double.parseDouble(val[1]), Double.parseDouble(val[2])));}   
				//(j, w) tuple is stored as keyvalue pair
		}	
		
		String i,j;
		Double Mik;
		Double Nkj;
		for(Entry<Double, Double> a : Entries_M)
		{      
			i = Double.toString(a.getKey());    
			Mik = a.getValue();      
			for (Entry<Double, Double> b : Entries_N)
			{  	 //Every value in M is multiplied with every value in N for the same key
				j = Double.toString(b.getKey());   	 
				Nkj = b.getValue();     		 
				outKey.set(i + "," + j);
				outVal.set(Double.toString(Mik*Nkj));
				context.write(outKey, outVal);
			}
		}
	}
    }
    public static class StepTwoMapper
	extends Mapper<Text, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outVal = new Text();
	public void map(Text key, Text value, Context context)
	    throws IOException, InterruptedException {
		outKey.set(key);
		outVal.set(value);
		context.write(outKey, outVal);
	}
    }
    public static class StepTwoReducer
	extends Reducer<Text, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outVal = new Text();
	public void reduce(Text key, Iterable<Text> values, Context context)
	    throws IOException, InterruptedException {
		Double total = 0.0;
		// For each key (i, k), emit the key value pair (i, k), v, 
		//where v is the sum of the list of values associated with this key 
		//and is the value of the element in row i and column k of the matrix P = MN.
		for (Text val : values){
			total += Double.parseDouble(val.toString());
								}
		outKey.set(key);
		outVal.set(Double.toString(total));
		context.write(outKey, outVal);
	  	}
    }
    
    
    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	// 1st Step
	Job jobOne = new Job(conf,"Step One");
	jobOne.setJarByClass(Multiply.class);
	jobOne.setOutputKeyClass(Text.class);
	jobOne.setOutputValueClass(Text.class);
	jobOne.setReducerClass(StepOneReducer.class);
	MultipleInputs.addInputPath(jobOne,new Path(args[0]),TextInputFormat.class,StepOneMapperM.class);
	MultipleInputs.addInputPath(jobOne, new Path(args[1]),TextInputFormat.class,StepOneMapperN.class);
	Path tempDir = new Path("temp");
	FileOutputFormat.setOutputPath(jobOne, tempDir);
	jobOne.waitForCompletion(true);
	
	// 2nd Step
	Job jobTwo = new Job(conf, "Step Two");
	jobTwo.setJarByClass(Multiply.class);
	jobTwo.setOutputKeyClass(Text.class);
	jobTwo.setOutputValueClass(Text.class);
	jobTwo.setMapperClass(StepTwoMapper.class);
	jobTwo.setReducerClass(StepTwoReducer.class);
	jobTwo.setInputFormatClass(KeyValueTextInputFormat.class);
	FileInputFormat.setInputPaths(jobTwo, tempDir);
	FileOutputFormat.setOutputPath(jobTwo, new Path(args[2]));
	jobTwo.waitForCompletion(true);
	FileSystem.get(conf).delete(tempDir, true);
    }
}
