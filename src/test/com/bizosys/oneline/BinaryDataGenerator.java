package com.bizosys.oneline;
/**
 * Bizosys Technologies Limited.
 * @author Abinasha Karana
 * @email abinash@bizosys.com
 * @website www.bizosys.com
 */
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Random;


/**
 * 
 * accountid	scrip		quantity	model1_price		model2_price		model3_price		..		model10_price	
 * 
 * compute total price for a scrip from model1 till model10. 
 *  	 
 */
public class BinaryDataGenerator {
	
	public static void main(String[] args) throws Exception {
		
		String fileName = args[0];
		int totalAccounts = new Integer(args[1]);
		int scrips = new Integer(args[2]);
		int models = new Integer(args[3]);

		Random randomGenerator = new Random(129212121121L);
		int basePrice = 0;
		float deviation = 0;
		
		FileOutputStream fos = new FileOutputStream(fileName);
		short recordSize = (short) (8 + ( (models+1)*4));
		
		OutputStream out = new BufferedOutputStream(fos);
		out.write(putShort(recordSize));
		
		for ( int a=0; a< totalAccounts; a++ ) {
			
			for ( int i=0; i< scrips; i++ ) {
				basePrice = 0;
				while ( basePrice == 0) {
					basePrice = randomGenerator.nextInt(4000);
				}
				out.write( putInt("ACC".hashCode()));
				out.write( putInt("SCRIP".hashCode()));
				out.write(putFloat(basePrice));
				
				for ( int j=0; j< models; j++) {
					deviation = 0;
					while ( deviation == 0) {
						deviation = randomGenerator.nextFloat();
					}
					out.write(putFloat(basePrice*deviation));
				}
			}			
		}

		
		out.close();
	}
	
	public static byte[] putFloat( float value ) {
		return putInt(Float.floatToRawIntBits(value));
	}
	

	/**
	 * Forms a byte array from a Integer data
	 * @param value	Integer data
	 * @return	4 bytes
	 */
	public static byte[] putInt( int value ) {
		return new byte[] { 
			(byte)(value >> 24), 
			(byte)(value >> 16 ), 
			(byte)(value >> 8 ), 
			(byte)(value) }; 
	}
	

	/**
	 * Forms a byte array from a Short data
	 * @param value	Short data
	 * @return	2 bytes
	 */
	public static byte[] putShort( short value ) {

		return new byte[] { 
			(byte)(value >> 8 & 0xff), 
			(byte)(value & 0xff) };
	}	
	
	
}
