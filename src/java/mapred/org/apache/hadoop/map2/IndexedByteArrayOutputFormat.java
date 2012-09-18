/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.map2;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.*;

import org.apache.hadoop.fs.Segment;

/** An {@link OutputFormat} that writes raw byte array and its indices. */
public class IndexedByteArrayOutputFormat<K, V> 
  extends FileOutputFormat<K, V> {

  protected <K, V> String generateIndexForKeyValue(
      K key, V value, String path) {
    return key.toString();
  }

  protected class ByteArrayRecordWriter<K, V>
    extends RecordWriter<K, V> {

    protected DataOutputStream out;
    protected DataOutputStream indexOut;
    protected String path;

    public ByteArrayRecordWriter(DataOutputStream out, 
                                 DataOutputStream indexOut, 
                                 String path) {
      this.out = out;
      this.indexOut = indexOut;
      this.path = path;
    }

    public synchronized void write(K key, V value)
      throws IOException {

      boolean nullKey = key == null || key instanceof NullWritable;
      boolean nullValue = value == null || value instanceof NullWritable;
      if (nullKey && nullValue) {
        return;
      }

      int prevSize = out.size();

      //do not write key, only write value, key is for index

      if (!(value instanceof byte[]))
        throw new IOException("Only byte[] supported as value type");

      if (!nullValue) {
        out.write((byte[])value);
      }
      int currSize = out.size();

      String index = generateIndexForKeyValue(key, value, path);
      indexOut.write(IndexingConstants.IDX_START);
      Text.writeString(indexOut, index);
      indexOut.write(IndexingConstants.SEG_START);
      indexOut.writeLong(prevSize);
      indexOut.writeLong(currSize - prevSize);
      indexOut.write(IndexingConstants.IDX_END);
    }

    public synchronized 
    void close(TaskAttemptContext context) throws IOException {
      out.close();
      indexOut.close();
    }
  }

  public RecordWriter<K, V> 
         getRecordWriter(TaskAttemptContext job
                         ) throws IOException, InterruptedException {
    Configuration conf = job.getConfiguration();
    Path file = getDefaultWorkFile(job, "");
    Path idxFile = new Path(file.toString() + ".map2idx");
    FileSystem fs = file.getFileSystem(conf);
    FSDataOutputStream fileOut = fs.create(file, false);
    FSDataOutputStream idxOut = fs.create(idxFile, false);
    return new ByteArrayRecordWriter<K, V>(
        fileOut, idxOut, 
        new Path(getOutputPath(job), getOutputName(job)).toString());
  }

  //for test
  public RecordWriter<K, V> 
         getRecordWriter(String path
                         ) throws IOException, InterruptedException {
    FileOutputStream fos = new FileOutputStream(path);
    FileOutputStream idxFos = new FileOutputStream(path + ".map2idx");
    DataOutputStream fileOut = new DataOutputStream(fos);
    DataOutputStream idxOut = new DataOutputStream(idxFos);
    return new ByteArrayRecordWriter<K, V>(fileOut, idxOut, "");
  }
}

