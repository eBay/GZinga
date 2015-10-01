package io.gzinga;

import java.io.File;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestGZipRandomAccess {

	@Test
	public void testGZipOutputStream() {
		try {
			GZipOutputStreamRandomAccess gzip = new GZipOutputStreamRandomAccess(new File("./target/testfile"));
			byte[] str = "This is line\n".getBytes();
			for(int i = 1; i <= 10000; i++) {
				if(i % 100 == 0) {
					gzip.addOffset(i/100l);
				}
				gzip.write(str);
			}
			Assert.assertEquals(gzip.getOffsetMap().size(), 100);
			gzip.close();
			
			Assert.assertTrue(GZipInputStreamRandomAccess.isGzipRandomOutputFile(new File("./target/testfile")));
			Assert.assertFalse(GZipInputStreamRandomAccess.isGzipRandomOutputFile(new File("./target/test-classes/testfile1")));
			GZipInputStreamRandomAccess gzin = new GZipInputStreamRandomAccess(new File("./target/testfile"));
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
			Assert.fail();
		}
	}
}
