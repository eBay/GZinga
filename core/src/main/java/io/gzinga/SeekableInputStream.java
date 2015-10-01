/*
 * Copyright 2015 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
