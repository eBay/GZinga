package io.gzinga.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import io.gzinga.GZipOutputStreamRandomAccess;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSplittableGZipCodec {

	@Test
	public void testSplittableGZipCodec() {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "file:///");
			FileSystem fs = FileSystem.get(conf);
			fs.mkdirs(new Path("target/test"));
			GZipOutputStreamRandomAccess gzip = new GZipOutputStreamRandomAccess(
					fs.create(new Path("target/test/testfile1.gz")));
			String str = "This is line\n";
			for(int i = 1; i <= 10000; i++) {
				gzip.write(str.getBytes());
				if(i % 100 == 0) {
					gzip.addOffset(i/100l);
				}
			}
			Assert.assertEquals(gzip.getOffsetMap().size(), 100);
			gzip.close();
					
			conf.set("mapreduce.framework.name", "local");
			conf.set("io.compression.codecs","io.gzinga.hadoop.SplittableGZipCodec");
			conf.set("mapreduce.input.fileinputformat.split.maxsize", "20000");
			Job job = new Job(conf, "word count");
			job.setJarByClass(WordCount.class);
			job.setMapperClass(WordCount.TokenizerMapper.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path("target/test/testfile1.gz"));
			FileOutputFormat.setOutputPath(job, new Path("target/test/testfile2"));
			job.waitForCompletion(true);
			
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path("target/test/testfile2/part-r-00000"))));
			Assert.assertEquals("This\t10000", br.readLine());
			Assert.assertEquals("is\t10000", br.readLine());
			Assert.assertEquals("line\t10000", br.readLine());
			br.close();
		} catch(Exception e) {
			e.printStackTrace();
			Assert.fail();
		} finally {
				FileUtil.fullyDelete(new File("target/test/testfile2"));
				FileUtil.fullyDelete(new File("target/test/testfile1.gz"));			
		}
	}
}
