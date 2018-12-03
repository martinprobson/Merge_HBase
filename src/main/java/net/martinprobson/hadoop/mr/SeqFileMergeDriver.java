package net.martinprobson.hadoop.mr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileNotFoundException;
import java.io.IOException;


/**
 * Driver class for the Merge_HBase.
 * 
 * <h4>Summary</h4>
 * <p>This class implements the standard Hadoop Tool interface and expects to be passed 
 * two arguments specifying the input and output directories as follows:
 * <p>
 * Running on a cluster via yarn: -
 * <p>
 * <code>
 * yarn jar jar_name SeqFileMergeDriver input_dir output_dir
 * </code>
 * <p>Running locally: -
 * <p>
 * <code>
 * hadoop --config local_config_file SeqFileMergeDriver input_dir output_dir
 * </code>
 * <p>Both run configurations are setup in the project pom.xml as goals <code>exec:exec@run-cluster</code> and 
 * <code>exec:exec@run-local</code> respectively.<p>
 * 
 * @author martinr
 *
 */
public class SeqFileMergeDriver extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(SeqFileMergeDriver.class);
	
    /**
     * Execute a Map Reduce job.
     * 
     * @param args input directory, output directory.
     * @return exit code.
     * @throws Exception if an error occurs.
     */
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic_options] <input path> <output path>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Configuration c = getConf();
		c.setStrings("io.serializations", c.get("io.serializations"),
				"org.apache.hadoop.hbase.mapreduce.MutationSerialization",
				"org.apache.hadoop.hbase.mapreduce.ResultSerialization");
		Job job = Job.getInstance(c);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setJarByClass(SeqFileMergeDriver.class);
		job.setJobName("Seq File Merge");
		// Ensure that all files are read in the directory
		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// Check if output path exists and delete it if it does
		Path outputPath = new Path(args[1]);
		LOG.info("About to check if " + outputPath + " exists.");
		if (pathExists(getConf(),outputPath))
			deletePath(getConf(),outputPath);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);
		// Set single reducer task to merge the file(s)
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Result.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	static void deletePath(Configuration conf, Path name) {

		try {
			FileSystem.get(conf).delete(name, true);
		} catch (IOException var3) {
			System.exit(2);
		}

	}

	private static boolean pathExists(Configuration conf, Path name) {
		boolean rc = true;
		try {
			FileSystem.get(conf).getFileStatus(name);
		} catch (FileNotFoundException e) {
			rc = false;
		} catch (IOException e) {
			System.exit(2);
		}
		return rc;
	}

    /**
     *
     * Execute a Map Reduce job via command line.
     * 
     * @param args input directory, output directory.
	 * @throws Exception if an error occurs.
     */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SeqFileMergeDriver(),args);
		System.exit(exitCode);
	}
}
