package net.martinprobson.hadoop.mr;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;


public class TestMapReduce {
	
	/**
	 * The output directory for results.
	 */
	private static final String TEST_OUTPUT_DIR = "output";
	
	/**
	 * The Configutation object used for the test.
	 */
	private Configuration conf;
	
	/**
	 * Clean up output directory.
	 */
	@Before
	public void setUp() {
		conf = new Configuration();
	}
	
	
	@Test
	public void testMapReduce() throws Exception {
		
		// The input data dir needs to be in the Maven test/resources directory.
		Path input  = new Path(TestMapReduce.class.getResource("/input_testfiles").toString());
		Path output = new Path(TEST_OUTPUT_DIR);
				
		SeqFileMergeDriver.deletePath(conf,output);
		
		SeqFileMergeDriver driver = new SeqFileMergeDriver();
		driver.setConf(conf);
		
		int exitCode = driver.run(new String[] {
				input.toString(),
				output.toString()
		});
		assertEquals(0, exitCode);
		
		String p = TestMapReduce.class.getResource("/TestMapReduce_expected_results.txt").toURI().getPath();
		Path expectedResultsFile = new Path(p);
		Map<ImmutableBytesWritable, Result> expectedResults = readSeqFile(conf,expectedResultsFile);
		Map<ImmutableBytesWritable,Result> actualResults = readSeqFile(conf,new Path(TEST_OUTPUT_DIR + "/part-r-00000"));
		assertEquals(expectedResults.size(), actualResults.size());
		actualResults.forEach((k,v) -> assertTrue(expectedResults.containsKey(k)));
		actualResults.forEach((k,v) -> {
			try {
				Result.compareResults(v,expectedResults.get(k));
			} catch (Exception e) {
			    fail("actual result does not equal actual" + e);
			}
		});
	}
	
	/**
	 * Delete the output directory.
	 */
	@After
	public void tearDown() {
		SeqFileMergeDriver.deletePath(conf,new Path(TEST_OUTPUT_DIR));
	}

	private static Map<ImmutableBytesWritable, Result> readSeqFile(Configuration conf, Path fileName) throws IOException {
		Map<ImmutableBytesWritable, Result> lines = new TreeMap<>();
		Reader r = null;

		try {
			r = new Reader(conf, Reader.file(fileName));
			ImmutableBytesWritable key = (ImmutableBytesWritable) ReflectionUtils.newInstance(r.getKeyClass());

			while(r.next(key)) lines.put(key, (Result) r.getCurrentValue(new Result()));
		} finally {
			IOUtils.closeStream(r);
		}

		return lines;
	}
}
