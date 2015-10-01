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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;

/**
 * This class takes care of locating header in gzip file and finding out metadata.
 *
 * */
public class InputStreamConverter extends InputStream {
	private SeekableInputStream raf;
	private Map<Long, Long> offsetMap = new LinkedHashMap<Long, Long>();

	private static int readSize = 32 * 1024;
	public static byte[] headerbytes = new byte[] {
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
        System.getProperty("os.name").indexOf("win") >= 0 ? (byte)0: (byte)7     // Operating system (OS)
        
    };

	/**
	 * @param gzipFile SeekableInputStream object
	 * @param index index where it should jump first
	 * @param cl Class for type key
	 * @throws IOException
	 */
	public InputStreamConverter(SeekableInputStream gzipFile, Long index) throws IOException {
		this(gzipFile, true);
		jumpToIndex(index);
	}

	/**
	 * @param gzipFile SeekableInputStream object
	 * @param loadMetadata should loadMetadata at initialization
	 * @param cl Class for type key
	 * @throws IOException
	 */
	public InputStreamConverter(SeekableInputStream gzipFile, boolean loadMetadata) 
			throws IOException {
		raf = gzipFile;
		if(checkHeader() == false) {
			raf.close();
			throw new IllegalArgumentException("Not a GZIPRandomOutput file.");
		}
		if(loadMetadata) {
			storeMetadata();
		}
	}
	
	public long getPos() throws IOException {
		return raf.getPos();
	}

	public void resetPos(long pos) throws IOException {
		raf.seek(pos);
	}
		
	/**
	 * This method checks header to confirm whether this file is written for random access.
	 * @return
	 * @throws IOException
	 */
	private boolean checkHeader() throws IOException {
		long pos = raf.getPos();
		byte[] buf = new byte[headerbytes.length];
		int n = raf.read(buf);
		if(n < headerbytes.length) {
			return false;
		}
		n = lastIndexOf(buf, headerbytes, 0, buf.length - 1);
		raf.seek(pos);
		if(n == -1) {
			return false;
		} else {
			return true;
		}
	}
	
	/**
	 * Writes metadata information into Map.
	 * @throws IOException
	 */
	private void storeMetadata() throws IOException {
		long pos = 0;
		int position = 0;
		int bytesToRead = readSize;
		if (raf.getLen() < readSize) {
			position = 0;
			bytesToRead = (int)raf.getLen();
		} else {
			position = (int) (raf.getLen() - readSize);
		}
		while(true) {
			raf.seek(position);
			byte[] bytes = new byte[bytesToRead];
			int lastIndex = 0;
			int totalLen = 0;
			while(true) {
				int len = raf.read(bytes, lastIndex, bytesToRead - lastIndex);
				totalLen += len;
				if(totalLen != bytesToRead) {
						lastIndex = totalLen;
				} else {
					break;
				}
			}
			int headerIndex = lastIndexOf(bytes, headerbytes, 0, bytesToRead - 1);
			if(headerIndex == -1) {
				if(position == 0) {
					raf.seek(0);
					break;
				}
				bytesToRead = position;
				if(bytesToRead > readSize) {
					bytesToRead = readSize;
				}
				position -= (readSize - headerbytes.length);
				if(position < 0) {
					position = 0;
				}
			} else {
				raf.seek(position + headerIndex + headerbytes.length);
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				while(true) {
					int i = raf.read();
					if(i == 0) {
						break;
					}
					bos.write(i);
				}
				String metadata = bos.toString();
				StringTokenizer stk = new StringTokenizer(metadata, ";");
				while(stk.hasMoreTokens()) {
					String token = stk.nextToken();
					if(token.isEmpty()) {
						continue;
					}
					int index1 = token.indexOf(":");
					if(index1 == -1) {
						continue;
					}
					try {
						offsetMap.put(Long.valueOf(token.substring(0, index1)), 
								Long.valueOf(token.substring(index1 + 1)));
					} catch (Exception e) {
						throw new IllegalArgumentException("Error in generating metadata", e);
					}
				}
				break;
			}
		}
		raf.seek(pos);
	}

	public Map<Long, Long> getMetadata() {
		return this.offsetMap;
	}
	
	public void jumpToIndex(Long index) throws IOException {
		Long offset = offsetMap.get(index);
		if(offset != null) {
			raf.seek(offset);
		} else {
			raf.seek(0);
		}
	}
	
	public static int lastIndexOf(byte[] srcData, byte[] dataToFind, int startIndex, int endIndex) {
        int iDataToFindLen = dataToFind.length;
        int iMatchDataCntr = iDataToFindLen-1;
        for (int counter = endIndex; counter >= startIndex; counter--) {
            if (srcData[counter] == dataToFind[iMatchDataCntr]) {
                iMatchDataCntr--;
            } else {
                iMatchDataCntr = iDataToFindLen-1;
            }
            if (iMatchDataCntr == 0) {
                return counter-1;
            } 
        }
        return -1;
    }
	
	public static int firstIndexOf(byte[] srcData, byte[] dataToFind, int startIndex, int endIndex) {
        int iDataToFindLen = dataToFind.length;
        int iMatchDataCntr = 0;
        for (int counter = startIndex; counter <= endIndex; counter++) {
            if (srcData[counter] == dataToFind[iMatchDataCntr]) {
                iMatchDataCntr++;
            } else {
                iMatchDataCntr = 0;
            }
            if (iMatchDataCntr == iDataToFindLen) {
                return counter - iDataToFindLen + 1;
            } 
        }
        return -1;
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
		return raf.available();
		//	return (int)(raf.getLen() - raf.getPos());
	}

}
