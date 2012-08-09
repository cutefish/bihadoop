/* Synthetic micro benchmark
 */
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableFactories;

public class MicroBench {
  public static final Log LOG = LogFactory.getLog(MicroBench.class);

  /* PerfResult */
  static class PerfResult implements Writable {
    final private int PERFID_STR_LEN = 8;
    final private int READQUERYTIME_STR_LEN = 15;
    final private int READREFTIME_STR_LEN = 13;
    final private int COMPUTETIME_STR_LEN = 13;
    final private int RESULTSAMPLE_STR_LEN = 14;

    private long perfId;
    private long readQueryTime;
    private long readRefTime;
    private long computeTime;
    private long resultSample;

    PerfResult() {
      perfId = 0;
      readQueryTime = 0;
      readRefTime = 0;
      computeTime = 0;
      resultSample = 0;
    }

    PerfResult(long perfId, long readQueryTime, long readRefTime, 
        long computeTime, long resultSample) {
      this.perfId = perfId;
      this.readQueryTime = readQueryTime;
      this.readRefTime = readRefTime;
      this.computeTime = computeTime;
      this.resultSample = resultSample;
    }

    static {
      WritableFactories.setFactory(PerfResult.class,
          new WritableFactory() {
            public Writable newInstance() { return new PerfResult(); }
      });
    }

    public void write(DataOutput out) throws IOException {
      out.writeChars("perfId: " + Long.toString(perfId) + "\n");
      out.writeChars("readQueryTime: " + Long.toString(readQueryTime) + "\n");
      out.writeChars("readRefTime: " + Long.toString(readRefTime) + "\n");
      out.writeChars("computeTime: " + Long.toString(computeTime) + "\n");
      out.writeChars("resultSample: " + Long.toString(resultSample) + "\n");
    }

    public void readFields(DataInput in) throws IOException {
      perfId = Long.parseLong(
          in.readLine().substring(PERFID_STR_LEN));
      readQueryTime = Long.parseLong(
          in.readLine().substring(READQUERYTIME_STR_LEN));
      readRefTime = Long.parseLong(
          in.readLine().substring(READREFTIME_STR_LEN));
      computeTime = Long.parseLong(
          in.readLine().substring(COMPUTETIME_STR_LEN));
      computeTime = Long.parseLong(
          in.readLine().substring(RESULTSAMPLE_STR_LEN));
    }
  }
  
  //data generate
  private void genData(String fileName, int size) 
    throws IOException {
    File file = new File(fileName);
    if (file.isDirectory()) throw new IOException(fileName + " is a directory");
    if (file.isFile() && file.length() > size) return;

    LOG.info("Generating data file: " + fileName +
        " size: " + size);

    Random r = new Random();
    FileOutputStream out = new FileOutputStream(file);
    byte[] buf = new byte[4096];
    int bytesWrite = 0;
    while (bytesWrite < size) {
      r.nextBytes(buf);
      out.write(buf);
      bytesWrite += buf.length;
    }
  }

  //read data
  private int readFully(InputStream in, byte[] buf) throws IOException {
    int n = 0;
    int bytesRead = 0;
    while((n >= 0) && (bytesRead < buf.length)) {
      n = in.read(buf, bytesRead, buf.length);
      bytesRead += n;
    }
    return bytesRead;
  }

  //do the computation
  private int compute(byte[] query, byte[] ref, 
      int querySize, int refSize, byte[] out, int shift) {
    int ind = 0;
    for (int i = 0; i < refSize - querySize; i += shift) {
      out[ind] = 0;
      for (int j = 0; j < querySize; j++) {
        out[ind] += query[j] * ref[i + j];
      }
      ind ++;
    }
    return ind;
  }

  //perf
  private PerfResult perf(InputStream queryIn, InputStream refIn, 
      int querySize, int refSize, int shift) throws IOException {
    long start, end;
    PerfResult result = new PerfResult();
    result.perfId = System.currentTimeMillis();

    byte[] queryBuf = new byte[querySize];
    byte[] refBuf = new byte[refSize];
    byte[] outBuf = new byte[refSize];

    //read
    start = System.currentTimeMillis();
    int queryRealSize = readFully(queryIn, queryBuf);
    end = System.currentTimeMillis();
    result.readQueryTime = end - start;
    start = System.currentTimeMillis();
    int refRealSize = readFully(refIn, refBuf);
    end = System.currentTimeMillis();
    result.readRefTime = end - start;

    //do work
    start = System.currentTimeMillis();
    int outSize = compute(queryBuf, refBuf, 
        queryRealSize, refRealSize, outBuf, shift);
    end = System.currentTimeMillis();
    result.computeTime = end - start;

    result.resultSample = outBuf[outSize];

    return result;
  }

  public void perfSeqLocal(Configuration conf) throws IOException {

    String queryFile = conf.get("seq.local.query.filename", "queryFile");
    String refFile = conf.get("seq.local.ref.filename", "refFile");
    int querySize = conf.getInt("seq.local.query.size", 2048);
    int refSize = conf.getInt("seq.local.ref.size", 1048576);
    int shift = conf.getInt("seq.local.ref.shift", 2048);

    LOG.info("queryFile: " + queryFile);
    LOG.info("refFile: " + refFile);
    LOG.info("querySize: " + querySize);
    LOG.info("refSize: " + refSize);
    LOG.info("shift: " + shift);

    genData(queryFile, querySize);
    genData(refFile, refSize);

    FileInputStream queryIn = new FileInputStream(queryFile);
    FileInputStream refIn = new FileInputStream(refFile);

    PerfResult result = perf(queryIn, refIn, querySize, refSize, shift);

    result.write(new DataOutputStream(System.out));
  }

  public static void main(String[] args) {
    System.out.println("Microbench");
    if (args.length < 1) {
      System.out.println("usage: java MicroBench benchname");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    conf.addResource("microbench.conf.xml");

    MicroBench mb = new MicroBench();

    if (args[0].equals("perfSeqLocal")) {
      try {
        mb.perfSeqLocal(conf);
      }
      catch (Exception e) {
        System.out.println(e);
      }
    }
    else {
      System.out.println("Invalid function: " + args[0]);
    }
  }

}
