package com.bizosys.oneline;
/**
 * Bizosys Technologies Limited.
 * @author Abinasha Karana
 * @email abinash@bizosys.com
 * @website www.bizosys.com
 */

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;

public class BinaryReader {
  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

  private InputStream in;
  private byte[] buffer;

  private int bufferSize = DEFAULT_BUFFER_SIZE;
  private int bufferLength = 0; // the number of bytes of real data in the buffer
  private int bufferPosn = 0; // the current position in the buffer

  /**
   * Create a line reader that reads from the given stream using the
   * default buffer-size (64k).
   * @param in The input stream
   * @throws IOException
   */
  public BinaryReader(InputStream in) {
    this(in, DEFAULT_BUFFER_SIZE);
  }

  /**
   * Create a line reader that reads from the given stream using the 
   * given buffer-size.
   * @param in The input stream
   * @param bufferSize Size of the read buffer
   * @throws IOException
   */
  public BinaryReader(InputStream in, int bufferSize) {
    this.in = in;
    this.bufferSize = bufferSize;
    this.buffer = new byte[this.bufferSize];
  }

  /**
   * Create a line reader that reads from the given stream using the
   * <code>io.file.buffer.size</code> specified in the given
   * <code>Configuration</code>.
   * @param in input stream
   * @param conf configuration
   * @throws IOException
   */
  public BinaryReader(InputStream in, Configuration conf) throws IOException {
    this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
  }

  /**
   * Close the underlying stream.
   * @throws IOException
   */
  public void close() throws IOException {
    in.close();
  }
  
  /**
   * Read 1 chunk from the InputStream into the given byte array.  
   *
   * @param str the object to store the given line (without newline)
   *  the rest of the line is silently discarded.
   *  
   * @param blockEndPosition the maximum number of bytes to consume
   *  in this call.  We will never overshoot this boundary.
   *
   * @return the number of bytes read 
   *
   * @throws IOException if the underlying stream throws
   */
  private static byte ZERO_BYTE = (byte) 0;
  int chunkSize = -1;
  int consumed = 0;
  int bufferRemaining = -1;
  int remainingRead = -1;
  public int readChunk(byte[] output, long leftoutBlockBytes) throws IOException {

    Arrays.fill(output, ZERO_BYTE );
    chunkSize = output.length;
    consumed = 0;
    
    while ( true ) {
    	//Can serve from buffer
    	bufferRemaining = bufferLength - bufferPosn;
    	remainingRead = chunkSize - consumed;
    	if ( bufferRemaining >=  remainingRead ) {
    		System.arraycopy(buffer, bufferPosn, 
    			output, consumed, remainingRead);
    		bufferPosn = bufferPosn + remainingRead;
    		return chunkSize;
    	}
    		
    	//Can Partially read from buffer
    	if ( bufferRemaining > 0 ) {
    		System.arraycopy(buffer, bufferPosn, output, consumed, bufferRemaining);
    		consumed = consumed + bufferRemaining;
    	}

    	//Read buffer again
    	bufferPosn = bufferLength = bufferRemaining = 0;
        bufferLength = in.read(buffer); //Buffer is at end, fill this up.
        if ( bufferLength <= 0 ) {
        	if (consumed <= 0 ) return 0;
        	throw new IOException(
        			"Unable to read left over bytes : consumed = " 
        			+  consumed + " , bufferLength = "+ bufferLength
        			+  " , remainingRead = "+ remainingRead
        			+  " , buffer remaining = " + bufferRemaining);
        }
    }
  }

 }