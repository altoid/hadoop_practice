package wordcount.cfinputformat;

import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import java.io.IOException;

public class WCCFInputFormat 
    extends CombineFileInputFormat<LongWritable, Text>
{
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException
    {
        return new CombineFileRecordReader<LongWritable, Text>((CombineFileSplit)split, context, WCCFRecordReader.class);
    }
}
