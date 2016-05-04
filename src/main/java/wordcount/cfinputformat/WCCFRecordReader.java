package wordcount.cfinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class WCCFRecordReader extends RecordReader<LongWritable, Text>
{
    private LongWritable   m_currentKey;
    private Text           m_currentValue;
    private Path           m_path;
    private FileSystem     m_currentFS;
    private Configuration  m_config;
    private BufferedReader m_currentReader;
    private int            m_numPaths;
    private int            m_currentLine;
    private long           m_length;
    private long           m_bytesRead;

    /**
     * 
     * @param cfsplit
     * @param context
     * @param idx
     *            - i THINK this is the index of the path we are to process,
     *            which implies that this RR will process one path at a time in
     *            the split.
     */
    public WCCFRecordReader(CombineFileSplit cfsplit, TaskAttemptContext context, Integer idx)
    {
        System.out.printf("############# WCCFRecordReader ctor >>>>>>>>>>>>>>\n");
        System.out.printf("idx:  %d\n", idx);
        m_length = cfsplit.getLength(idx);
        m_numPaths = cfsplit.getNumPaths();
        System.out.printf("getNumPaths:  %d\n", m_numPaths);
        m_bytesRead = 0;
        m_path = cfsplit.getPath(idx);

        System.out.printf("\tgetLength[%d]:  %d\n", idx, cfsplit.getLength(idx));
        System.out.printf("\tgetOffset[%d]:  %d\n", idx, cfsplit.getOffset(idx));
        System.out.printf("\tgetPath[%d]:  %s\n", idx, m_path);

        System.out.printf("<<<<<<<<<<<<< WCCFRecordReader ctor ##############\n");

        // this whole thing should be well-behaved if path count is 0,
        // deal with that later.
        m_config = context.getConfiguration();

    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
    {
        CombineFileSplit cfsplit = (CombineFileSplit) split;

        m_currentLine = 1;

        // should we get the FS object from path.getFileSystem(config)?

        m_currentFS = FileSystem.get(m_config);
        m_currentReader = new BufferedReader(new InputStreamReader(m_currentFS.open(m_path)));
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException
    {
        return m_currentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException
    {
        return m_currentValue;
    }

    /**
     * i think this records the progress through this path (indicated by the idx
     * arg to the ctor), not progress through the whole split.
     */
    @Override
    public float getProgress() throws IOException, InterruptedException
    {
        return m_bytesRead / (float) m_length;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
        if (m_currentReader == null)
        {
            return false;
        }

        String line = m_currentReader.readLine();
        if (line != null)
        {
            // TODO: avoid creating new Text for each line read,
            // see if we can reuse one object.
            m_currentKey = new LongWritable(m_currentLine);
            m_currentValue = new Text(line);
            m_currentLine++;
            m_bytesRead += (line.length() + 1); // add 1 for the \n which is
                                                // chopped off
            return true;
        }

        m_currentReader.close();
        m_currentReader = null;

        return false;
    }

    @Override
    public void close() throws IOException
    {

    }
}
