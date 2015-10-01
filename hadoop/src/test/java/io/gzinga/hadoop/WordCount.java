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

package io.gzinga.hadoop;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

	  public static class TokenizerMapper 
	  	extends Mapper<Object, Text, Text, IntWritable>{
	      private final static IntWritable one = new IntWritable(1);
	      private Text word = new Text();
	      public void map(Object key, Text value, Context context
	                      ) throws IOException, InterruptedException {
	    	  StringTokenizer itr = new StringTokenizer(value.toString());
	    	  while (itr.hasMoreTokens()) {
	          word.set(itr.nextToken());
	          context.write(word, one);
	        }
	      }
	    }
	    
	    public static class IntSumReducer 
	         extends Reducer<Text,IntWritable,Text,IntWritable> {
	      private IntWritable result = new IntWritable();
	  
	      public void reduce(Text key, Iterable<IntWritable> values, 
	                         Context context
	                         ) throws IOException, InterruptedException {
	        int sum = 0;
	        for (IntWritable val : values) {
	          sum += val.get();
	        }
	        result.set(sum);
	        context.write(key, result);
	      }
	    }
}
