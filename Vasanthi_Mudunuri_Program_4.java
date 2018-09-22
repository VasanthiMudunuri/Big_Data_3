import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Vasanthi_Mudunuri_Program_4 {
public static int startline=2;
public static int count=1;
        public static class JoinCFSMapper extends Mapper<LongWritable,Text,TextPair,Text>
        {
                public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
                {
                      if(count>startline) //to omit reading headers from file
                       {
                        String[] record=value.toString().split(",");
                        Text statename=new Text(record[2]);
                        Text tons=new Text(record[8]);
                        Text tag=new Text("1");
                        context.write(new TextPair(statename,tag),tons); //generating output keyvalue pairs
                       }
                   count++;
                }
        }
        public static class JoinACSMapper extends Mapper<LongWritable,Text,TextPair,Text>
        {
        public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
                {
                        if(count>startline) //to omit reading headers from file
                        {
                        String[] record=value.toString().split(",");
                        Text statename=new Text(record[2]);
                        Text population=new Text(record[3]);
                        Text tag=new Text("0");
                        context.write(new TextPair(statename,tag),population); //generating output keyvalue pairs
                        }
                  count++;
                }
        }

        public static class ReduceSideJoinReducer extends Reducer<TextPair,Text,Text,Text>
        {
                public void reduce(TextPair key,Iterable<Text> values,Context context) throws IOException,InterruptedException
                {
                        Iterator<Text> value=values.iterator();
                        Text population=new Text(value.next());
                        int totaltons=0;
                        while(value.hasNext()) //iterating through values
                        {
                                String record=value.next().toString();
                                totaltons+=NumberUtils.toInt(record,0); //calculating totaltons
                        }
                        Text outputvalue=new Text(population.toString()+"\t"+String.valueOf(totaltons));
                        context.write(new Text(key.getFirst()+"\t"),outputvalue); //generating output keyvalue pairs
                }
        }
        public static class ReduceSideJoinDriver extends Configured implements Tool{
                public class keyPartitioner extends Partitioner<TextPair,Text>
                {
                        public int getPartition(TextPair key,Text value,int numPartitions) //partitoner
                        {
                                return(key.getFirst().hashCode() & Integer.MAX_VALUE)%numPartitions;
                        }
                }
                public int run(String[] args) throws Exception
                {
                        if(args.length!=3)
                        {
                                System.err.println("please provide datasets inputpaths and outputpath as arguments"); //checking for the required arguments
                                return -1;
                        }
                        Configuration conf=new Configuration();
                        Job job=Job.getInstance(conf,"ReduceSide Join");
                        job.setJarByClass(Vasanthi_Mudunuri_Program_4.class);
                        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class, JoinACSMapper.class);
                        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class, JoinCFSMapper.class);
                        job.setPartitionerClass(keyPartitioner.class); //setting partitioner
                        job.setGroupingComparatorClass(TextPair.FirstComparator.class); //setting comparator
                        job.setReducerClass(ReduceSideJoinReducer.class);
                        job.setMapOutputKeyClass(TextPair.class);
                        job.setMapOutputValueClass(Text.class);
                        job.setOutputKeyClass(Text.class);
                        job.setOutputValueClass(Text.class);
                        job.setOutputFormatClass(MapFileOutputFormat.class); //mapfile as output
                        FileOutputFormat.setOutputPath(job,new Path(args[2]));
                        return job.waitForCompletion(true)? 0:1;
                }
        }
        public static void main(String[] args) throws Exception{
                int exitCode = ToolRunner.run(new ReduceSideJoinDriver(), args);
                System.exit(exitCode);
        }

}
