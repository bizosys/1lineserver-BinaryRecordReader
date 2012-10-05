package com.bizosys.oneline;
/**
 * Bizosys Technologies Limited.
 * @author Abinasha Karana
 * @email abinash@bizosys.com
 * @website www.bizosys.com
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class BinaryInputFormat extends FileInputFormat<LongWritable, BytesWritable> {

  @Override
  public RecordReader<LongWritable, BytesWritable> 
    createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new BinaryRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false;
  }

}