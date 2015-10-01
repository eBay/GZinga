#GZinga - Seekable and Splittable GZip 

Generally data compression techniques are used in industry in order to conserve space and network bandwidth. Some of widely used compression techniques are  gzip, bzip2, lzop, 7-zip etc. According to performance benchmarks, lzop is one of fastest compression algorithm while gzip (with lowest level compression) compression is as fast or faster than serial writes on disk and achieves a way better compression ratio than lzop. Bzip2 has high compression rate but is very slow. For decompression also, gzip performs better than other algorithms (ref [1, 2]). That is reason why gzip is one of the most popular and widely used data compression techniques. But limitation with gzip is you cannot randomly access gzip file and you cannot split gzip file in case Hadoop map-reduce jobs. This makes it slower in those scenarios and does not leverage power of parallel processing. This project is aim to provide Seekable (random search within gzip file) and Splittable (can divide gzip file into multiple chunks) capability for gzip compressed file.

## Motivation
It is common to collect logs from production applications and these are used to debug and triage issues. Generally log files are rolled over periodically (hourly, daily) or based on size.  These files are stored in compressed format like gzip to save disk space. In most outage scenario, developers are interested in looking at logs for particular activity or around certain timestamp. Scanning entire file to find out log for particular time is costly. The data for these logs can be stored on commodity storage like Hadoop. On Hadoop, there is need to leverage parallel processing to process small chunks of a large gzip file in parallel (for files beyond few hundred MBs). 

In order to provide best performance reading/processing of gzip file, idea of GZinga originated. Though we have looked at options for log files, the technique can apply to other usecases like textual documents, web crawls, weather data, etc.    

## Usage

### Requisites
 * Maven 

### Build
The following steps need to be followed in order to build the jar file :
 * Clone the project on GitHub
 * Do a maven build at the top level of the project using `mvn clean install`
 * If usecase is to generate index based gzip file and perform random search, then jar file will be available under gzinga-core/target/gzinga-core*.jar
 * If it needs to be used with Hadoop MR job to split file, then jar file will be available under gzinga-hadoop/target/gzinga-hadoop*.jar

### Running test cases
 * In order to run test cases use `mvn clean test`

### Seekable Gzip: Write
Class GZipOutputStreamRandomAccess which extends DeflaterOutputStream provides required methods to implement index data within file.

    class GZipOutputStreamRandomAccess extends DeflaterOutputStream {
        private Map<Long, Long> offsetMap = new LinkedHashMap<Long, Long>(); //will maintain index where value provides byte offset for given key
     
        /** This method adds current byte offset (in gzip file) for given key.*/
        public void addOffset(Long key) {
        }
 
        /**Writes header with extra comment which contains entries from offsetMap.*/
        private void writeHeaderWithComment() {
        }
    }

### Seekable Gzip: Read
GZipInputStreamRandomAccess which extends GZIPInputStream provides required methods to jump to specific locations in gzip file.

    class GZipInputStreamRandomAccess extends GZIPInputStream {

        /** Return metadata information for given file.*/   
        public Map<Long, Long> getMetadata(); 
	
        /** This method jump to location for specifies key. If specified key does not exist, then it will jump to beginning of file.*/
        public void jumpToIndex(Long index) throws IOException;
    }

If one needs to read from Hadoop, then he needs to use SeekableGZipDataInputStream class as shown below:

    FSDataInputStream fin = fs.open(new Path("testfile"));
    long len = fs.getFileStatus(new Path("testfile")).getLen();
    SeekableGZipDataInputStream sin = new SeekableGZipDataInputStream(fin, len);
    GZipInputStreamRandomAccess gzin = new GZipInputStreamRandomAccess(sin); 

### Splittable GZip
SplittableGZipCodec class implements SplittableCodec provided by Hadoop. If file is generated with multiple headers, then it will be able to split accordingly. If there is single gzip header, then it will run with single split only.

#### Configuration
 In order to use split feature for gzip file, one needs to set “io.compression.codec” to "io.gzinga.hadoop.SplittableGZipCodec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.SnappyCodec" for JobConf object.
 Also one can set split size by setting property "“mapreduce.input.fileinputformat.split.maxsize” to required value.
 

