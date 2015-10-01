package io.gzinga;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
/*import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;*/
import java.io.OutputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * This class implements a stream filter for writing compressed data in
 * the GZIP file format. Along with that, it writes extra metadata in each header.
 * In order to write extra metadata, 3rd bit of FLG will be set to 1.
 * 
 * User can invoke method addOffset(T key) which will insert an entry into HashMap
 * with value as current byte offset in compressed file. Every time, addOffset method is being 
 * called, all existing data will be flushed and new header will be written. This header will 
 * also have metadata information (maintained in HashMap).
 * 
 * addOffset method takes generic type argument, so user can have any object (like Long, String etc)
 * as key. User needs to provide constructor for that type which takes String as input in order to
 * convert into required object.
 * 
 * Using this metadata information, user can jump to any location in file using GZipInputStreamRandomAccess.
 *
 */
public class GZipOutputStreamRandomAccess extends DeflaterOutputStream {
	
	long totalLength = 0;
	private static byte[] headerWithComment = new byte[] {
        (byte) GZIPInputStream.GZIP_MAGIC,        // Magic number (short)
        (byte)(GZIPInputStream.GZIP_MAGIC >> 8),  // Magic number (short)
        Deflater.DEFLATED,        // Compression method (CM)
        (byte)(0 | (1 << 4)),     // Flags (FLG)
        0,                        // Modification time MTIME (int)
        0,                        // Modification time MTIME (int)
        0,                        // Modification time MTIME (int)
        0,                        // Modification time MTIME (int)
        0,                        // Extra flags (XFLG)
        System.getProperty("os.name").indexOf("nux") >= 0 ? (byte)3: 
        	System.getProperty("os.name").indexOf("win") >= 0 ? (byte)0: (byte)7 // Operating system (OS)
        
    };
	
	/**
	 * HashMap for maintaing metadata information.
	 */
	Map<Long, Long> offsetMap = new LinkedHashMap<Long, Long>();

    /**
     * CRC-32 of uncompressed data.
     */
    protected CRC32 crc = new CRC32();

    /*
     * Trailer size in bytes.
     *
     */
    private final static int TRAILER_SIZE = 8;

   /**
     * Creates a new output stream with the specified buffer size and
     * flush mode.
     *
     * @param out the output stream
     * @param size the output buffer size
     * @exception IOException If an I/O error has occurred.
     * @exception IllegalArgumentException if {@code size <= 0}
     *
     * @since 1.7
     */
    public GZipOutputStreamRandomAccess(OutputStream out, int size) throws IOException {
        super(out, new Deflater(Deflater.DEFAULT_COMPRESSION, true),
              size,
              true);
        writeHeader();
        crc.reset();
    }
    
    public GZipOutputStreamRandomAccess(File gzipFile) throws IOException {
    	this(new FileOutputStream(gzipFile));
    }
    
    /**
     * Creates a new output stream with a default buffer size.
     *
     * <p>The new output stream instance is created as if by invoking
     * the 2-argument constructor GZIPOutputStream(out, false).
     *
     * @param out the output stream
     * @exception IOException If an I/O error has occurred.
     */
    public GZipOutputStreamRandomAccess(OutputStream out) throws IOException {
        this(out, 512);   
    }

    /**
     * Invoking this method will flush all data into stream. It will insert new entry into metadata 
     * for specified key and current byte location in stream. After that, it will write new header in
     * compressed file with metadata information.
     * @param key
     * @throws IOException
     */
    public void addOffset(Long key) throws IOException {
		resetGzipStream();
		offsetMap.put(key, totalLength);
    	writeHeader();
    }
    
    /**
     * This method returns current metadata information.
     * @return Map<T, Long> with exisitng metadata information.
     */
    public Map<Long, Long> getOffsetMap() {
    	return Collections.unmodifiableMap(offsetMap);
    }
    
	/**
     * Writes array of bytes to the compressed output stream. This method
     * will block until all the bytes are written.
     * @param buf the data to be written
     * @param off the start offset of the data
     * @param len the length of the data
     * @exception IOException If an I/O error has occurred.
     */
    public synchronized void write(byte[] buf, int off, int len) throws IOException {
        super.write(buf, off, len);
        crc.update(buf, off, len);
    }

    /**
     * This method flushes all data and resets compress stream.
     * @throws IOException
     */
    public void resetGzipStream() throws IOException {
		finish();
		def.reset();
    }
    
    @Override
    public void close() throws IOException {
		resetGzipStream();
    	writeHeader();
    	super.close();
    }
    
    /**
     * This method writes header into compressed stream. In this, 3rd bit in FLG will be set and metadata
     * information will be written at end of header with special byte '0' at end.
     * @throws IOException
     */
    public void writeHeader() throws IOException {
    	out.write(headerWithComment);
    	totalLength += headerWithComment.length;
    	StringBuffer sb = new StringBuffer();
    	for(Map.Entry<Long, Long> entry : offsetMap.entrySet()) {
    		sb.append(entry.getKey()).append(":").append(entry.getValue()).append(";");
    	}
    	out.write(sb.toString().getBytes());  
    	totalLength += sb.toString().getBytes().length;
    	out.write(new byte[]{0});
    	totalLength += 1;
    	crc.reset();
    }
	
    /**
     * Finishes writing compressed data to the output stream without closing
     * the underlying stream. Use this method when applying multiple filters
     * in succession to the same output stream.
     * @exception IOException if an I/O error has occurred
     */
    public void finish() throws IOException {
        if (!def.finished()) {
            def.finish();
            while (!def.finished()) {
                int len = def.deflate(buf, 0, buf.length);
                if (def.finished() && len <= buf.length - TRAILER_SIZE) {
                    // last deflater buffer. Fit trailer at the end
                    writeTrailer(buf, len);
                    len = len + TRAILER_SIZE;
                    totalLength += TRAILER_SIZE;
                    out.write(buf, 0, len);
                    return;
                }
                if (len > 0) {
                    out.write(buf, 0, len);
                }
            }
            // if we can't fit the trailer at the end of the last
            // deflater buffer, we write it separately
            byte[] trailer = new byte[TRAILER_SIZE];
            writeTrailer(trailer, 0);
            out.write(trailer);
            totalLength += TRAILER_SIZE;
        }
    }

    /*
     * Writes GZIP member trailer to a byte array, starting at a given
     * offset.
     */
    private void writeTrailer(byte[] buf, int offset) throws IOException {
        writeInt((int)crc.getValue(), buf, offset); // CRC-32 of uncompr. data
        writeInt(def.getTotalIn(), buf, offset + 4); // Number of uncompr. bytes
        totalLength += def.getTotalOut();
    }

    /*
     * Writes integer in Intel byte order to a byte array, starting at a
     * given offset.
     */
    private void writeInt(int i, byte[] buf, int offset) throws IOException {
        writeShort(i & 0xffff, buf, offset);
        writeShort((i >> 16) & 0xffff, buf, offset + 2);
    }

    /*
     * Writes short integer in Intel byte order to a byte array, starting
     * at a given offset
     */
    private void writeShort(int s, byte[] buf, int offset) throws IOException {
        buf[offset] = (byte)(s & 0xff);
        buf[offset + 1] = (byte)((s >> 8) & 0xff);
    }    
    
    public static void main(String[] args) throws Exception {
	//	GZipOutputStreamRandomAccess gzip = new GZipOutputStreamRandomAccess(new File(args[1]));
    	GZIPOutputStream gzip = new GZIPOutputStream(new FileOutputStream(args[1]), true);
		
    	InputStream br = new FileInputStream(args[0]);
		int count = 0;
		long lines = Long.valueOf(args[2]).longValue() / 60;
		long j = 0;
		while(true) {
			byte[] buf = new byte[100 * 1024];
			int len = br.read(buf, 0, buf.length);
			if(len <= 0) {
				break;
			}
			gzip.write(buf, 0, len);
			count += len;
			if(count >= lines ) {
				gzip.flush();
	//			gzip.addOffset(j++);
				count = 0;
			}
		}
		System.out.println("offset" + j);
		br.close();
		gzip.close();
    }
}
