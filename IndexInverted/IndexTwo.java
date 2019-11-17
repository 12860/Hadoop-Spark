import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class IndexTwo {

	public static class MapTwo extends Mapper<LongWritable,Text,Text,Text>
	{
		@Override
		protected void map(LongWritable key, Text value,Context context)
			throws IOException , InterruptedException {
				String line = value.toString();
				String[] filds = StringUtils.split(line,"\t");

				String[] words = StringUtils.split(filds[0],"-->");

				String word = words[0];

				String filename = words[1];
				long count = Long.parseLong(filds[1]);
				context.write(new Text(word), new Text(filename+"-->"+count));
			}
	}


	public static class ReducerTwo extends Reducer<Text,Text,Text,Text>
	{
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException,InterruptedException{
				String link = "";
				for(Text value:values){
					link+=value+" ";
				}
				context.write(key,new Text(link));
			}
	}


	public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(IndexTwo.class);
		job.setMapperClass(MapTwo.class);
		job.setReducerClass(ReducerTwo.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job,new Path(args[0]));

		Path output = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);

		if(fs.exists(output)){
			fs.delete(output,true);
		}
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		System.exit(job.waitForCompletion(true)?0:1);

	}


}
