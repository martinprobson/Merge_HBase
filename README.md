# HBase Export File Merge

## Summary

Simple map reduce that can merge multiple sequence files produced by the HBase export utility into a single sequence file.
The `hdfs dfs -getmerge` command does not deal with
 [SequenceFile](https://hadoop.apache.org/docs/r2.7.3/api/org/apache/hadoop/io/SequenceFile.html) formats correctly.

The HBase [Export](https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/Export.html) utility produces 
sequence files with a key class of [ImmutableBytesWritable](https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/io/ImmutableBytesWritable.html)
and a value class of [Result](https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Result.html)




