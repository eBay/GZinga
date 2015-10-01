package io.gzinga;

import java.io.InputStream;
import java.io.IOException;

/**
 * This class defines required methods which will be used InputStreamConverter for doing random
 * access. As different implementations (like RandomAccessFile and FSDataInputStream) have
 * different methods for providing same functionality, this common abstract class will provide
 * uniformity.
 *
 */
public abstract class SeekableInputStream extends InputStream {

	/**
	 * This method jumps to specified offset in given InputStream.
	 * @param offset byte location where it should jump.
	 * @throws IOException if any error occurs.
	 */
	public abstract void seek(long offset) throws IOException;
	
	/**
	 * This method returns current file pointer position.
	 * @return Current file pointer position.
	 * @throws IOException if any error occurs
	 */
	public abstract long getPos() throws IOException;
	
	/**
	 * This method returns total file size for given input stream.
	 * @return total file length in bytes
	 */
	public abstract long getLen();
}
