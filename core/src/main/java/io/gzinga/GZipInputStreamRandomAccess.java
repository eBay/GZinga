package io.gzinga;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.zip.CheckedInputStream;
import java.util.zip.ZipException;
import java.util.zip.GZIPInputStream;


/**
 * This class reads compressed file written using GZipOutputStreamRandomAccessFile. File will have extra
 * metadata information in each header which provides information about byte offset for different keys.
 * Depending upon which key needs to be accessed, first random access to specified byte location will be performed
 * and then compressed stream will be read. 
 *
 */
public class GZipInputStreamRandomAccess  extends GZIPInputStream {

    private final static int FHCRC      = 2;    // Header CRC
    private final static int FEXTRA     = 4;    // Extra field
    private final static int FNAME      = 8;    // File name
    private final static int FCOMMENT   = 16;   // File comment

	/**
	 * Input stream object should be of type SeekableInputStream. User also needs to provide Class
	 * to specify type of key.
	 * @param is
	 * @param cl
	 * @throws IOException
	 */
    public GZipInputStreamRandomAccess(SeekableInputStream is) throws IOException {
		this(is, true);
	}

    /**
     * Input stream object should be of type SeekableInputStream. User also needs to provide Class
	 * to specify type of key. If loadMetadata is false, then by default metadata will not be loaded
	 * while creating object itself.
     * @param is
     * @param loadMetadata
     * @param cl
     * @throws IOException
     */
	public GZipInputStreamRandomAccess(SeekableInputStream is, boolean loadMetadata) throws IOException {
		super(new InputStreamConverter(is, loadMetadata));
	}

	/**
	 * Location to gzipFile should be provided as first argument.
	 * @param gzipFile
	 * @param cl
	 * @throws IOException
	 */
	public GZipInputStreamRandomAccess(File gzipFile) throws IOException {
		this(new SeekableRandomAccessFile(gzipFile));
	}

	/**
	 * Input stream object should be of type SeekableInputStream. User also needs to provide Class
	 * to specify type of key. index argument specifies which location to jump.
	 * @param is
	 * @param index
	 * @param cl
	 * @throws IOException
	 */
	public GZipInputStreamRandomAccess(SeekableInputStream is, Long index) throws IOException {
		super(new InputStreamConverter(is, index));
	}

	/**
	 * File argument provides location to a file. User also needs to provide Class to specify
	 * type of key. index argument specifies which location to jump.
	 * @param gzipFile
	 * @param index
	 * @param cl
	 * @throws IOException
	 */
	public GZipInputStreamRandomAccess(File gzipFile, Long index) throws IOException {
		this(new SeekableRandomAccessFile(gzipFile), index);
	}

	/**
	 * Return metadata information for given file.
	 * @return
	 */
	public Map<Long, Long> getMetadata() {
		return Collections.unmodifiableMap(((InputStreamConverter)this.in).getMetadata());
	}
	
	/**
	 * This method jump to location for specifies key. If specified key does not exist, then it
	 * will jump to beginning of file.
	 * @param index
	 * @throws IOException
	 */
	public void jumpToIndex(Long index) throws IOException {
		((InputStreamConverter)this.in).jumpToIndex(index);
		readHeader();
	}
	
	/**
	 * Return current position in file.
	 * @return
	 * @throws IOException
	 */
	public long getPos() throws IOException {
		return ((InputStreamConverter)this.in).getPos();
	}
	
	/**
	 * Reset file pointer position to specified location.
	 * @param pos
	 * @throws IOException
	 */
	public void resetPos(long pos) throws IOException {
		((InputStreamConverter)this.in).resetPos(pos);	
	}
	
    /*
     * Reads unsigned short in Intel byte order.
     */
    private int readUShort(InputStream in) throws IOException {
        int b = readUByte(in);
        return ((int)readUByte(in) << 8) | b;
    }

    /*
     * Reads unsigned byte.
     */
    private int readUByte(InputStream in) throws IOException {
        int b = in.read();
        if (b == -1) {
            throw new EOFException();
        }
        if (b < -1 || b > 255) {
            // Report on this.in, not argument in; see read{Header, Trailer}.
            throw new IOException(this.in.getClass().getName()
                + ".read() returned value out of range -1..255: " + b);
        }
        return b;
    }
    
    private byte[] tmpbuf = new byte[128];
    /*
     * Skips bytes of input data blocking until all bytes are skipped.
     * Does not assume that the input stream is capable of seeking.
     */
    private void skipBytes(InputStream in, int n) throws IOException {
        while (n > 0) {
            int len = in.read(tmpbuf, 0, n < tmpbuf.length ? n : tmpbuf.length);
            if (len == -1) {
                throw new EOFException();
            }
            n -= len;
        }
    }
    
    /*
     * Reads GZIP member header and returns the total byte number
     * of this member header.
     */
	private int readHeader() throws IOException {
        CheckedInputStream in = new CheckedInputStream(this.in, crc);
        crc.reset();
        // Check header magic
        if (readUShort(in) != GZIP_MAGIC) {
            throw new ZipException("Not in GZIP format");
        }
        // Check compression method
        if (readUByte(in) != 8) {
            throw new ZipException("Unsupported compression method");
        }
        // Read flags
        int flg = readUByte(in);
        // Skip MTIME, XFL, and OS fields
        skipBytes(in, 6);
        int n = 2 + 2 + 6;
        // Skip optional extra field
        if ((flg & FEXTRA) == FEXTRA) {
            int m = readUShort(in);
            skipBytes(in, m);
            n += m + 2;
        }
        // Skip optional file name
        if ((flg & FNAME) == FNAME) {
            do {
                n++;
            } while (readUByte(in) != 0);
        }
        // Skip optional file comment
        if ((flg & FCOMMENT) == FCOMMENT) {
            do {
                n++;
            } while (readUByte(in) != 0);
        }
        // Check optional header CRC
        if ((flg & FHCRC) == FHCRC) {
            int v = (int)crc.getValue() & 0xffff;
            if (readUShort(in) != v) {
                throw new ZipException("Corrupt GZIP header");
            }
            n += 2;
        }
        crc.reset();
        return n;
    }
	
	/**
	 * Checks whether provided file is of type random access.
	 * @param file
	 * @return
	 * @throws FileNotFoundException
	 */
	public static boolean isGzipRandomOutputFile(File file) throws FileNotFoundException {
		return isGzipRandomOutputFile(new SeekableRandomAccessFile(file));
	}
	
	/**
	 * Checks whether provided input stream is of type random access.
	 * @param gzipFile
	 * @return
	 */
	public static boolean isGzipRandomOutputFile(SeekableInputStream gzipFile) {
		InputStreamConverter is = null;
		try {
			is = new InputStreamConverter(gzipFile, false);
		} catch(IllegalArgumentException e) {
			return false;
		} catch(IOException ie) {
			return false;
		} finally {
			if(is != null) {
				try {
					is.close();
				} catch(Exception e) {
				}
			}
		}
		return true;
	}	
	
	public static void main(String[] args) throws Exception {
	//	GZipInputStreamRandomAccess gzin = new GZipInputStreamRandomAccess(new File(args[0]));
		GZIPInputStream gzin = new GZIPInputStream(new FileInputStream(args[0]));
		//gzin.jumpToIndex(Long.valueOf(args[1]).longValue());
		//System.out.println(gzin.getPos());
		gzin.skip(Long.valueOf(args[1]).longValue());
		
		int count1 = 0;
		while(true) {
			int l = gzin.read();
			if(l == -1) {
				break;
			}
			count1++;
			if(count1 == 10) {
				break;
			}
		}
		System.out.println(count1);
		gzin.close();
	}
}

