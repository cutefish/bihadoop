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

/** An {@link OutputFormat} that writes plain text files. */
public abstract class IndexedTextOutputFormat<K, V> 
  extends FileOutputFormat<K, V> {

  abstract protected <K, V> String generateIndexForKeyValue(
      K key, V value, String path);

  protected class LineRecordWriter<K, V>
    extends RecordWriter<K, V> {
    private final String utf8 = "UTF-8";
    private final byte[] newline;

    protected DataOutputStream out;
    protected DataOutputStream indexOut;
    protected String path;
    private final byte[] keyValueSeparator;

    public LineRecordWriter(DataOutputStream out, DataOutputStream indexOut, 
                            String path, String keyValueSeparator) {
      try {
        newline = "\n".getBytes(utf8);
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
      }
      this.out = out;
      this.indexOut = indexOut;
      this.path = path;
      try {
        this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
      }
    }

    public LineRecordWriter(DataOutputStream out, DataOutputStream indexOut,
                            String path) {
      this(out, indexOut, path, "\t");
    }

    /**
     * Write the object to the byte stream, handling Text as a special
     * case.
     * @param o the object to print
     * @throws IOException if the write throws, we pass it on
     */
    private void writeObject(Object o) throws IOException {
      if (o instanceof Text) {
        Text to = (Text) o;
        out.write(to.getBytes(), 0, to.getLength());
      } else {
        out.write(o.toString().getBytes(utf8));
      }
    }

    public synchronized void write(K key, V value)
      throws IOException {

      boolean nullKey = key == null || key instanceof NullWritable;
      boolean nullValue = value == null || value instanceof NullWritable;
      if (nullKey && nullValue) {
        return;
      }

      int prevSize = out.size();

      if (!nullKey) {
        writeObject(key);
      }
      if (!(nullKey || nullValue)) {
        out.write(keyValueSeparator);
      }
      if (!nullValue) {
        writeObject(value);
      }
      out.write(newline);

      int currSize = out.size();

      String index = generateIndexForKeyValue(key, value, path);
      indexOut.write(IndexingConstants.INDEX_START);
      Text.writeString(indexOut, index);
      Segment segment = new Segment(new Path(path), prevSize, currSize - prevSize);
      indexOut.write(IndexingConstants.SEGMENT_START);
      segment.write(indexOut);
      indexOut.write(IndexingConstants.SEGMENT_END);
      indexOut.write(IndexingConstants.INDEX_END);
    }

    public synchronized 
    void close(TaskAttemptContext context) throws IOException {
      out.close();
      //write index
      indexOut.writeInt(indices.size());
      for (int i = 0; i < indices.size(); ++i) {
        Text.writeString(indexOut, indices.get(i));
        indexOut.writeInt(1);
        indexOut.writeLong(segments.get(i).getOffset());
        indexOut.writeLong(segments.get(i).getLength());
      }
      indexOut.close();
    }
  }

  public RecordWriter<K, V> 
         getRecordWriter(TaskAttemptContext job
                         ) throws IOException, InterruptedException {
    Configuration conf = job.getConfiguration();
    boolean isCompressed = getCompressOutput(job);
    String keyValueSeparator= conf.get("mapred.textoutputformat.separator",
                                       "\t");
    CompressionCodec codec = null;
    String extension = "";
    if (isCompressed) {
      Class<? extends CompressionCodec> codecClass = 
        getOutputCompressorClass(job, GzipCodec.class);
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
      extension = codec.getDefaultExtension();
    }
    Path file = getDefaultWorkFile(job, extension);
    Path idxFile = new Path(file.toString() + ".map2idx");
    FileSystem fs = file.getFileSystem(conf);
    if (!isCompressed) {
      FSDataOutputStream fileOut = fs.create(file, false);
      FSDataOutputStream idxOut = fs.create(idxFile, false);
      return new LineRecordWriter<K, V>(
          fileOut, idxOut, 
          new Path(getOutputPath(job), getOutputName(job)).toString(),
          keyValueSeparator);
    } else {
      FSDataOutputStream fileOut = fs.create(file, false);
      FSDataOutputStream idxOut = fs.create(idxFile, false);
      return new LineRecordWriter<K, V>(
          new DataOutputStream(codec.createOutputStream(fileOut)), idxOut, 
          new Path(getOutputPath(job), getOutputName(job)).toString(),
          keyValueSeparator);
    }
  }
}

