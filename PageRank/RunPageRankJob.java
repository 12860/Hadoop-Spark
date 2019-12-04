

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tyx on 2017/11/29.
 */
public class RunPageRankJob {

//    统计所有链接的入链
    private static Map<String,String > allInLine = new HashMap<>();
//    统计所有链接的出链
    private static Map<String,Integer> allOutLine = new HashMap<>();
//    统计所有链接的现有pagerank
    private static Map<String,Double> allPageRank = new HashMap<>();
//    统计所有链接计算后的pagerank
    private static Map<String ,Double> allNextPageRank = new HashMap<>();

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
//            configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
        //configuration.set("fs.defaultFS", "hdfs://node1:8020");
        //configuration.set("yarn.resourcemanager.hostname", "node1");
//        第一个MapReduce为了统计出每个页面的入链，和每个页面的出链数
        if (run1(configuration)){
            run2(configuration);
        }
    }

    /*
    输入数据：
    A    B    D
    B    C
    C    A    B
    D    B    C*/

    static class AcountOutMapper extends Mapper<Text,Text,Text,Text>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
//            super.map(key, value, context);
            int num = 0;
//            若A能连接到B，则说明B是A的一条出链
            String[] outLines = value.toString().split("\t");
            for (int i=0;i<outLines.length;i++){
                context.write(new Text(outLines[i]),key);
            }
            num = outLines.length;
//            统计出链
            context.write(key,new Text("--"+num));
        }
    }

    static class AcountOutReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            super.reduce(key, values, context);
//            统计该页面的入链页面
            String outStr = "";
            int sum = 0;
            for (Text text : values){
//                统计出链数目
                if (text.toString().contains("--")){
                    sum += Integer.parseInt(text.toString().replaceAll("--",""));
                }else {
                    outStr += text+"\t";
                }
            }
            context.write(key,new Text(outStr+sum));
            allOutLine.put(key.toString(),sum);
            allInLine.put(key.toString(),outStr);
            allPageRank.put(key.toString(),1.0);
        }
    }

    public static boolean run1(Configuration configuration){
        try {
            Job job = Job.getInstance(configuration);
            FileSystem fileSystem = FileSystem.get(configuration);

            job.setJobName("acountline");

            job.setJarByClass(RunPageRankJob.class);

            job.setMapperClass(AcountOutMapper.class);
            job.setReducerClass(AcountOutReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setInputFormatClass(KeyValueTextInputFormat.class);

            Path intPath = new Path("/usr/output/page_rank_data.txt");
            FileInputFormat.addInputPath(job,intPath);


            Path outPath = new Path("/usr/output/acoutline");
            if (fileSystem.exists(outPath)){
                fileSystem.delete(outPath,true);
            }
            FileOutputFormat.setOutputPath(job,outPath);

            boolean f = job.waitForCompletion(true);
            return f;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /*第一次MapReduce输出数据：
    A    C
    B    A   C   D
    C    B   D
    D    A*/

    static class PageRankMapper extends Mapper<Text,Text,Text,Text>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
//            super.map(key, value, context);
            String myUrl = key.toString();
//            取出该页面所有的入链页面
            String inLines = allInLine.get(myUrl);
            context.write(key,new Text(inLines));
        }
    }

    static class PageRankReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            super.reduce(key, values, context);
//            后半段求和公式的和 （PR(1)/L(1)…………PR(i)/L(i)
            double sum = 0.0;
            String outStr = "";
            for (Text text : values){
                String[] arr = text.toString().split("\t");
                for (int i=0;i<arr.length;i++){
                    outStr += arr[i]+"\t";
                    sum += allPageRank.get(arr[i])/allOutLine.get(arr[i]);
                }
            }
//            算出该页面本次的PR结果
            double nowPr = (1-0.85)/allPageRank.size()+0.85*sum;

            allNextPageRank.put(key.toString(),nowPr);
            context.write(key,new Text(outStr));
        }
    }

    public static void run2(Configuration configuration){
        double d = 0.001;
        int i=1;
//        迭代循环趋于收敛
        while (true){
            try {
                configuration.setInt("count",i);
                i++;
                Job job = Job.getInstance(configuration);
                FileSystem fileSystem = FileSystem.get(configuration);

                job.setJobName("pagerank");
                job.setJarByClass(RunPageRankJob.class);
                job.setJobName("Pr"+i);

                job.setMapperClass(PageRankMapper.class);
                job.setReducerClass(PageRankReducer.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                job.setInputFormatClass(KeyValueTextInputFormat.class);

                Path intPath = new Path("/usr/output/pagerank.txt");
                if (i>2){
                    intPath = new Path("/usr/output/Pr"+(i-1));
                }
                FileInputFormat.addInputPath(job,intPath);


                Path outPath = new Path("/usr/output/Pr"+i);
                if (fileSystem.exists(outPath)){
                    fileSystem.delete(outPath,true);
                }
                FileOutputFormat.setOutputPath(job,outPath);

                boolean f = job.waitForCompletion(true);
                if (f){
                    System.out.println("job执行完毕");
                    double sum = 0.0;
//                    提取本轮所有页面的PR值和上一轮作比较，
                    for (String key : allPageRank.keySet()){
                        System.out.println(key+"--------------------------"+allPageRank.get(key));
                        sum += Math.abs(allNextPageRank.get(key)-allPageRank.get(key));
                        allPageRank.put(key,allNextPageRank.get(key));
                    }
                    System.out.println(sum);
//                    若平均差小于d则表示收敛完毕
                    if (sum/allPageRank.size()<d){
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
