package io.gzinga.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import io.gzinga.GZipInputStreamRandomAccess;
import io.gzinga.GZipOutputStreamRandomAccess;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHadoopGZipRandomAccess {

	@Test
	public void testGZipOutputStream() {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "file:///");
			FileSystem fs = FileSystem.get(conf);
			fs.mkdirs(new Path("target/test"));
			GZipOutputStreamRandomAccess gzip = new GZipOutputStreamRandomAccess(
					fs.create(new Path("target/test/testfile")));
			byte[] str = "This is line\n".getBytes();
			for(int i = 1; i <= 10000; i++) {
				if(i % 100 == 0) {
					gzip.addOffset(i/100l);
				}
				gzip.write(str);
			}
			Assert.assertEquals(gzip.getOffsetMap().size(), 100);
			gzip.close();
			fs.copyFromLocalFile(new Path(fs.getWorkingDirectory().toString() + "/target/test-classes/testfile1"),
					new Path("target/test/testfile1"));
			FSDataInputStream fin = fs.open(new Path("target/test/testfile"));
			long len = fs.getFileStatus(new Path("target/test/testfile")).getLen();
			SeekableGZipDataInputStream sin = new SeekableGZipDataInputStream(fin, len);
			Assert.assertTrue(GZipInputStreamRandomAccess.isGzipRandomOutputFile(sin));
			fin = fs.open(new Path("target/test/testfile1"));
			sin = new SeekableGZipDataInputStream(fin, len);
			Assert.assertFalse(GZipInputStreamRandomAccess.isGzipRandomOutputFile(sin));
			fin = fs.open(new Path("target/test/testfile"));
			sin = new SeekableGZipDataInputStream(fin, len);
			GZipInputStreamRandomAccess gzin = new GZipInputStreamRandomAccess(sin);
			Assert.assertEquals(gzin.getMetadata().size(), 100);
			Assert.assertTrue(gzin.getMetadata().containsKey(1l));
			Assert.assertTrue(gzin.getMetadata().containsKey(100l));
			Assert.assertFalse(gzin.getMetadata().containsKey(200l));
			gzin.jumpToIndex(50l);
			int count1 = 0;
			while(true) {
				int l = gzin.read();
				if(l == -1) {
					break;
				}
				count1++;
			}
			gzin.jumpToIndex(60l);
			int count2 = 0;
			while(true) {
				int l = gzin.read();
				if(l == -1) {
					break;
				}
				count2++;
			}
			Assert.assertTrue(count1 > count2);
			gzin.close();
		} catch(Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
}
