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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndexOne {

    public static class MapOne extends Mapper<LongWritable, Text,Text,LongWritable>
    {
            @Override
    protected void map(LongWritable key, Text value,Context context)
        throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = StringUtils.split(line," ");

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            FileSplit inputSplit = (FileSplit)context.getInputSplit();

            String filename = inputSplit.getPath().getName();

            for(String word:words){
                context.write(new Text(word+"-->"+filename),new LongWritable(1));

            }
        }
    }



    public static class ReduceOne extends Reducer<Text, LongWritable,Text,LongWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,
            Context context) throws IOException, InterruptedException{
            long count = 0;
            for(LongWritable value:values){
                count += value.get();
            }
            context.write(key,new LongWritable(count));
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(IndexOne.class);

        job.setMapperClass(MapOne.class);
        job.setReducerClass(ReduceOne.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

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
