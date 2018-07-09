import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Pi {
	
	private static double radius = 0f;
	private final static IntWritable one = new IntWritable(1);
	private static Text word = new Text();
	private static long insideCount;
	private static long outsideCount;
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String xy;
				xy = tokenizer.nextToken();
				xy = xy.substring(1,4);
				int xvalue = Character.getNumericValue(xy.charAt(0));
				int yvalue = Character.getNumericValue(xy.charAt(2));
				double check = Math.sqrt(Math.pow((radius-xvalue),2) + Math.pow((radius-yvalue), 2));
				
				if(check < radius){
					word.set("inside");
				}else {
					word.set("outside");
				}
				context.write(word, one);
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
			if(key.find("inside")==0){
				insideCount = sum;
			}else{
				outsideCount = sum;
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		Scanner sc = new Scanner(System.in);
		System.out.print("Enter a radious:");
		radius = sc.nextDouble();
		System.out.print("Enter a total index pair of (x,y):");
		int index = sc.nextInt();
		
		int numX[] = new int[index];
		int numY[] = new int[index];
		sc.close();

		for (int i = 0; i < index; i++){
			numX[i] = (int) (Math.random() * (radius + 1));
			numY[i] = (int) (Math.random() * (radius + 1));
			System.out.println(numX[i] + "," + numY[i]);
		}
		
		File file = new File(args[0] + "/Input4.txt");
		file.createNewFile();
		FileWriter writer = new FileWriter(file);
		
		for (int i = 0; i < index; i++){
			writer.write("(" + numX[i] + "," + numY[i] + ") ");
		}
		writer.flush();
		writer.close();
		
		Configuration conf = new Configuration();
		String Input = args[0];
		String Output = args[1];
		Path input = new Path(Input);
		Path output = new Path(Output);
		Job job = new Job(conf, "Pi");
		job.setJarByClass(Pi.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);
		
		double pi = 4 * insideCount /(insideCount + outsideCount);
		System.out.println("Value of PI : " + pi);
	}
}
