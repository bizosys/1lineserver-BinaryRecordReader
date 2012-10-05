package com.bizosys.oneline;
/**
 * Bizosys Technologies Limited.
 * @author Abinasha Karana
 * @email abinash@bizosys.com
 * @website www.bizosys.com
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;


public class BinaryDataGeneratorCheck {
	/**
	 * Form a short value reading 2 bytes
	 * @param startPos	Bytes read start position
	 * @param inputBytes	Input Bytes
	 * @return	Short representation
	 */
	public static short getShort(int startPos, byte[] inputBytes) {
		return (short) (
			(inputBytes[startPos] << 8 ) + ( inputBytes[++startPos] & 0xff ) );
	}
	
	/**
	 * Form a integer value reading 4 bytes
	 * @param index	Bytes read start position
	 * @param inputBytes	Input Bytes
	 * @return	Integer representation
	 */
	public static int getInt(int index, byte[] inputBytes) {
		
		int intVal = (inputBytes[index] << 24 ) + 
		( (inputBytes[++index] & 0xff ) << 16 ) + 
		(  ( inputBytes[++index] & 0xff ) << 8 ) + 
		( inputBytes[++index] & 0xff );
		return intVal;
	}

	public static float getFloat( int index, byte[] inputBytes ) {
		return Float.intBitsToFloat(getInt(index, inputBytes));
	}
	
	
	public static void main(String[] args) throws Exception  {
		File aFile = new File(args[0]);
		BinaryReader reader = null;
		InputStream stream = null;
		int lineNo = 0;
		
		
		try {
			lineNo++;
			stream = new FileInputStream(aFile); 
			reader = new BinaryReader( stream);
			
			byte[] chunkSizeB = new byte[2];
			long len = aFile.length();
			reader.readChunk(chunkSizeB, len);
			short chunkSize = getShort(0, chunkSizeB);
			System.out.println("Chunk Size = " + chunkSize + " , Length=" + len);
			long records = (len - 2) / chunkSize;
			System.out.println("Total Records = " + records);
			
			long start = System.currentTimeMillis();
			
			byte[] bytes = new byte[chunkSize];
			int readLen = 0;
			for ( long l = 0; l < records; l++) {
				readLen = reader.readChunk(bytes, len);
				if ( readLen == 0 ) {
					System.out.println("Read till end : " + readLen);
					break;
				}
				int position = getInt(8, bytes);
				int index = 12;
				double modelT = 0;
				for ( int i=0; i<97; i++) {
		    	  float modelVal = getFloat(index, bytes);
		    	  //System.out.println(modelVal);
		    	  index = index + 4;
		    	  modelT = modelT + modelVal*position;
		    	  if ( index == chunkSize) break;
				}
				//if ( l % 1000 == 0 ) System.out.println(l);
			}
			
			long end = System.currentTimeMillis();
			
			System.out.println( "Time taken : " + (end - start));

		} catch (Exception ex) {
			ex.printStackTrace(System.err);	
			throw new RuntimeException(ex);
		} finally {
			try {if ( null != reader ) reader.close();
			} catch (Exception ex) {ex.printStackTrace(System.err);}
			try {if ( null != stream) stream.close();
			} catch (Exception ex) {ex.printStackTrace(System.err);}
		}
	}
	
}
