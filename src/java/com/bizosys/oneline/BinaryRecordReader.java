package com.bizosys.oneline;
/**
 * Bizosys Technologies Limited.
 * @author Abinasha Karana
 * @email abinash@bizosys.com
 * @website www.bizosys.com
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class BinaryRecordReader extends RecordReader<LongWritable, BytesWritable> {

  private long start;
  private long pos;
  private long end;
  private short recordSize = -1;
  private BinaryReader in;
  private LongWritable key = null;
  private BytesWritable value = null;

  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    this.start = split.getStart();
    this.end = start + split.getLength();
    /**
    if ( LOG.isInfoEnabled() ) LOG.info(
    	"Binary Split start/ends = " + start + "/" + end);
    */
    
    //open the file and seek to the start of the split
    final Path file = split.getPath();
    FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(split.getPath());
    if ( start != 0 ) fileIn.seek(start);
    this.pos = start;

    in = new BinaryReader(fileIn, job);
    if ( this.end != fileIn.available()) {
    	/**
    	LOG.info("Available Byte ( " + fileIn.available() + 
    		" and end bytes are not same. (" +  this.end + ")");
    	*/    	
    }
    
    //Read the File header
    this.recordSize = fileIn.readShort();
    this.pos += 2;
    if ( this.recordSize <= 0 ) {
    	throw new IOException("Corrupted File, Unknown Record Length : " + split.getPath());
    } else {
    	//LOG.info("A Record is of size : " + this.recordSize);    	
    }
  }
  
  public boolean nextKeyValue() throws IOException {
    if (key == null) key = new LongWritable();
    //key.set(pos);
    
    if (value == null) value = new BytesWritable(new byte[recordSize]);
    int newSize = 0;
	newSize = in.readChunk(value.getBytes(), (end-pos));
	pos += newSize;
    if (newSize == 0) {
      key = null;
      value = null;
      return false;
    } else {
      return true;
    }
  }

  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  public BytesWritable getCurrentValue() {
    return value;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
    	float prog = Math.min(1.0f, (pos - start) / (float)(end - start));
    	//LOG.info("Progress :" + prog);
    	return prog;
    }
  }
  
  public synchronized void close() throws IOException {
    if (in != null) {
      in.close(); 
    }
  }
}