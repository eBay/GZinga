package io.gzinga.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import io.gzinga.SeekableInputStream;

/*
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ebayopensource.compression.GZipInputStreamRandomAccess;
*/

/**
 * This class implements methods for SeekableInputStream with use of FSDataInputStream.
 * This class should be used in case of accessing hadoop file system.
 *
 */
public class SeekableGZipDataInputStream extends SeekableInputStream {

	FSDataInputStream fsIn;
	private long len = -1l;
	
	public SeekableGZipDataInputStream(FSDataInputStream in) {
		this.fsIn = in;
	}

	public SeekableGZipDataInputStream(FSDataInputStream in, long len) {
		this.fsIn = in;
		this.len = len;
	}
	
	@Override
	public void seek(long offset) throws IOException {
		fsIn.seek(offset);
	}

	@Override
	public long getPos() throws IOException {
		return fsIn.getPos();
	}

	@Override
	public long getLen() {
		if(this.len != -1) {
			return this.len;
		} else {
			try {
				return fsIn.available();
			} catch(IOException e) {
				return 0;
			}
		}
	}

	@Override
	public int read() throws IOException {
		return fsIn.read();
	}

	@Override
    public int read(byte[] buf, int off, int len) throws IOException {
    	return fsIn.read(buf, off, len);
    }
    
    @Override
    public int read(byte[] b) throws IOException {
    	return fsIn.read(b);
    }
    

	@Override
	public void close() throws IOException {
		fsIn.close();
		super.close();
	}
	
	@Override
	public int available() throws IOException {
		if(this.len == -1) {
			return fsIn.available();
		} else {
			return (int)(this.len - fsIn.getPos());
		}
	}
	
	/*public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://artemis-nn.vip.ebay.com:8020");
		FileSystem fs = FileSystem.get(conf);
		long time = System.currentTimeMillis();
		FSDataInputStream fin = fs.open(new Path(args[0]));
		long len = fs.getFileStatus(new Path(args[0])).getLen();
		SeekableGZipDataInputStream sin = new SeekableGZipDataInputStream(fin, len);
		GZipInputStreamRandomAccess gzin = new GZipInputStreamRandomAccess(sin);
		System.out.println(gzin.getMetadata());
		gzin.jumpToIndex(80l);
		System.out.println(gzin.getPos());
		BufferedReader br = new BufferedReader(new InputStreamReader(gzin));
		System.out.println(br.readLine());
		br.close();
		System.out.println("Time " + (System.currentTimeMillis() - time));
		
		time = System.currentTimeMillis();
		fin = fs.open(new Path(args[1]));
		len = Long.valueOf(args[2]).longValue();
		GZIPInputStream gz = new GZIPInputStream(fin);
		System.out.println("jumping to " + (long)(len * 0.8));
		gz.skip((long)(len * 0.8));
		System.out.println(fin.getPos());
		br = new BufferedReader(new InputStreamReader(gz));
		System.out.println(br.readLine());
		br.close();
		System.out.println("Time " + (System.currentTimeMillis() - time));
	}*/

}
