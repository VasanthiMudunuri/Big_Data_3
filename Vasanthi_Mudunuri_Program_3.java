import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Vasanthi_Mudunuri_Program_3 {
        public static class MapSideJoinMapper extends Mapper<LongWritable,Text,Text,Text>
        {
                HashMap<String,String> ACSmap=new HashMap<String,String>(); //map to store ACS statename and population
                String totalPopulation="";  //variable totalpopulation 
                Text outputKey=new Text(""); //variable outputkey
                Text outputValue=new Text(""); //variable outputvalue
                int startline=2;   //variable startline to start reading file from third line removing headers
                int count=1;    //varaible count for counting line in file
                public void setup(Context context) throws IOException, InterruptedException //setup is called before beginning of task
                {
                        Path path= new Path("ACS_15_5YR_S0101_with_ann.csv");  //creating file path
                        String lineRead="";  //variable to read line
                        FileSystem hdfs = FileSystem.getLocal(context.getConfiguration()); //to get filesystem configuration
                        BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(path))); //to read file
                        while((lineRead=br.readLine())!=null) //iterating through each line in file 
                        {
                                if(count>startline)
                                {
                                        String[] statePopulationArray=lineRead.split(","); //splitting line by ',' as seperator
                                        String state=statePopulationArray[2];   //storing third string in state
                                        String population=statePopulationArray[3]; //storing fourth string in population
                                        ACSmap.put(state, population); //adding to ACS map
                                }
                                count++;
                        }
                        br.close(); 
                }
                public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
                {
                                String[] CFSArray=value.toString().split(","); //splitting by ',' as seperator
                                String statename=CFSArray[2];  //storing third string in statename
                                if(statename.equals("GEO.display-label") || statename.equals("Geographic area name")) //to avoid headers in file
                                {
                                 return;
                                }
                                String tons=CFSArray[8];  //storing ninth string in tons
                                totalPopulation=ACSmap.get(statename); //retrieving totalpopulation from ACS map by statename as key
                                outputKey.set(statename);  //setting output key
                                outputValue.set(tons+"\t"+totalPopulation); //setting output value
                                context.write(outputKey,outputValue); //emitting keyvalue pairs
                }
        }
        public static class MapSideJoinReducer extends Reducer<Text,Text,Text,Text>
        {
                public void reduce(Text text,Iterable<Text> values,Context context) throws IOException,InterruptedException
                {
                        Iterator<Text> tonsandpopulation=values.iterator();  //to iterate through values
                        ArrayList<Integer> tons=new ArrayList<Integer>(); //list to store tons
                        Integer totaltons=0;         //variable totaltons
                        Integer totalpopulation=0;   //varaible totalpopulation
                        while(tonsandpopulation.hasNext())  //iterating through values
                        {
                                String[] record=tonsandpopulation.next().toString().split("\t"); //splitting by tab as seperator
                                int ton=NumberUtils.toInt(record[0],0);  //to store integers in ton
                                tons.add(ton);    //adding to tons 
                                totalpopulation=NumberUtils.toInt(record[1],0); //to store totalpopulation
                        }
                        for(Integer ton:tons) //iterating through tons
                        {
                                totaltons+=ton; //calculationg totaltons
                        }
                        context.write(new Text(text),new Text(totalpopulation+"\t"+totaltons)); //emitting output keyvalue pairs
                }
        }
		 public static class MapSideJoinDriver extends Configured implements Tool{
                public int run(String[] args) throws Exception
                {
                        if(args.length!=2)
                        {
                                System.err.println("please provide inputpath and outputpath as arguments"); //to check for required arguments
                                return -1;
                        }
                        Configuration conf=new Configuration();
                        Job job=Job.getInstance(conf,"MapSide Join");
                        job.setJarByClass(Vasanthi_Mudunuri_Program_3.class);
                        job.setMapperClass(MapSideJoinMapper.class);
                        job.addCacheFile(new Path("/CS5433/PA3/ACS/ACS_15_5YR_S0101_with_ann.csv").toUri()); //adding file to cache
                        job.setReducerClass(MapSideJoinReducer.class);
                        job.setMapOutputKeyClass(Text.class);
                        job.setMapOutputValueClass(Text.class);
                        job.setOutputKeyClass(Text.class);
                        job.setOutputValueClass(Text.class);
                        job.setOutputFormatClass(SequenceFileOutputFormat.class);
                        SequenceFileOutputFormat.setCompressOutput(job,true);
                        SequenceFileOutputFormat.setOutputCompressionType(job,CompressionType.BLOCK); //block compression
                        SequenceFileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);
                        FileInputFormat.addInputPath(job,new Path(args[0]));
                        FileOutputFormat.setOutputPath(job,new Path(args[1]));
                        return job.waitForCompletion(true)? 0:1;
                }
        }
        public static void main(String[] args) throws Exception{
                int exitCode = ToolRunner.run(new MapSideJoinDriver(), args);
                System.exit(exitCode);
        }
}

		
