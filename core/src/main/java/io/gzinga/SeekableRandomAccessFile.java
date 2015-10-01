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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * This class implements methods for SeekableInputStream with use of RandomAccessFile.
 * This class should be used in local mode.
 *
 */
public class SeekableRandomAccessFile extends SeekableInputStream {

	RandomAccessFile raf;
	private long len;
	
	public SeekableRandomAccessFile(File file) throws FileNotFoundException {
		raf = new RandomAccessFile(file, "r");
		len = file.length();
	}
	
	@Override
	public long getLen() {
		return this.len;
	}
	
	@Override
	public void seek(long offset) throws IOException {
		raf.seek(offset);
	}

	@Override
	public long getPos() throws IOException {
		return raf.getFilePointer();
	}

	@Override
	public int read() throws IOException {
		return raf.read();
	}

	@Override
    public int read(byte[] buf, int off, int len) throws IOException {
    	return raf.read(buf, off, len);
    }
    
    @Override
    public int read(byte[] b) throws IOException {
    	return raf.read(b);
    }
    

	@Override
	public void close() throws IOException {
		raf.close();
		super.close();
	}
	
	@Override
	public int available() throws IOException {
		return (int)(this.len - raf.getFilePointer());
	}
}
