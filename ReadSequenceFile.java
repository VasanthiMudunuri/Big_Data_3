import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class ReadSequenceFile {
        public static void main(String[] args) throws IOException {
                String sequencefile = args[0]; //taking sequencefile from arguments passed
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(URI.create(sequencefile), conf); 
                Path path = new Path(sequencefile); //creating path
                SequenceFile.Reader reader = null;
                try
                {
                        reader = new SequenceFile.Reader(conf,SequenceFile.Reader.file(path)); //creating file reader
                        Writable key = (Writable)
                                        ReflectionUtils.newInstance(reader.getKeyClass(), conf); //getting key
                        Writable value = (Writable)
                                        ReflectionUtils.newInstance(reader.getValueClass(), conf); //getting value
                        while (reader.next(key, value))
                        {
                                System.out.printf("%s\t%s\n", key, value); //printing output
                        }
                }
                finally {
                        IOUtils.closeStream(reader); //closing reader
                }
        }
}
