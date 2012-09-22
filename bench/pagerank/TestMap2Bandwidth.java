import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

public class TestMap2Bandwidth {
  public static void main(String[] args) throws Exception {
    if (args.length != 7) {
      System.out.println("java TestMap2Bandwidth " + 
                         "<fsUri> " + 
                         "<path> <off> <len> "+
                         "<path> <off> <len> " );
      System.exit(-1);
    }

    URI fsUri = new URI(args[0]);
    Path seg1Path = new Path(args[1]);
    long seg1Off = Integer.parseInt(args[2]);
    long seg1Len = Integer.parseInt(args[3]);
    Path seg2Path = new Path(args[4]);
    long seg2Off = Integer.parseInt(args[5]);
    long seg2Len = Integer.parseInt(args[6]);

    Configuration conf = new Configuration();
    conf.addResource("pagerank-conf.xml");
    FileSystem fs = FileSystem.get(fsUri, conf);
    int blockSize = conf.getInt("pagerank.block.size", 1);
    int numNodes = conf.getInt("pagerank.num.nodes", 1);

    FSDataInputStream in;
    BufferedReader reader;

    BufferedOutputStream out = new BufferedOutputStream(
        new FileOutputStream("/tmp/testmap2bandwidthout"));

    long start, end;

    /**
     * We store the previous rank in a array then combine them into a byte
     * array.
     */
    int arraySize = (int) (blockSize * 1.5);
    int[] rowIdArray = new int[arraySize];
    Arrays.fill(rowIdArray, -1);
    double[] prevRank = new double[arraySize];
    start = System.currentTimeMillis();
    in = fs.open(seg1Path);
    in.seek(seg1Off);
    reader = new BufferedReader(new InputStreamReader(in));
    int bytesRead = 0;
    int count = 0;
    while(bytesRead < seg1Len) {
      String lineText = reader.readLine();
      if (lineText == null) break;
      bytesRead += lineText.length() + 1;
      //ignore comment and blank line
      if (lineText.startsWith("#")) continue;
      if (lineText.equals("")) continue;

      String[] line = lineText.split("\t");
      //ignore ill-formed lines
      try {
        int rowId = Integer.parseInt(line[0]);
        int rowIdInBlock = rowId / (numNodes / blockSize);
        double rank = Double.parseDouble(line[1]);
        //prevRank.put(rowId, rank);
        prevRank[rowIdInBlock] = rank;
        rowIdArray[rowIdInBlock] = rowId;
        count ++;
      }
      catch(Exception e) {
        System.out.println("" + e + ", on line: " + lineText);
      }
    }
    //we add a header ahead of buffer to distinguish between previous and
    //current value
    ByteBuffer bbuf;
    bbuf = ByteBuffer.allocate(count * 12 + 4);
    bbuf.put((byte)0x55);
    bbuf.put((byte)0x00);
    bbuf.put((byte)0x55);
    bbuf.put((byte)0x00);
    for (int i = 0; i < arraySize; ++i) {
      if (rowIdArray[i] != -1) {
        bbuf.putInt(rowIdArray[i]);
        bbuf.putDouble(prevRank[i]);
      }
    }
    out.write(bbuf.array());
    in.close();
    end = System.currentTimeMillis();
    System.out.println("Processed node in " + (end - start) + " ms");
    System.out.println("Node processing bandwidth: " + bytesRead / (end - start) / 1000 + " MByte/s");

    /*** edge file ***/
    start = System.currentTimeMillis();
    in = fs.open(seg2Path);
    in.seek(seg2Off);

    DataInputStream dataIn = new DataInputStream(
        new BufferedInputStream(in));

    Arrays.fill(rowIdArray, -1);
    double[] rankArray = new double[arraySize];

    bytesRead = 0;
    while (bytesRead < seg2Len) {
      int rowId, colId, colIdInBlock;
      double xferProb, partialRank;
      try {
        rowId = dataIn.readInt();
        colId = dataIn.readInt();
        xferProb = dataIn.readDouble();
        bytesRead += 4 + 4 + 8;
        colIdInBlock = colId / (numNodes / blockSize);
        double rank = prevRank[colIdInBlock];
        partialRank = xferProb * rank;

        int rowIdInBlock = rowId / (numNodes / blockSize);
        rowIdArray[rowIdInBlock] = rowId;
        rankArray[rowIdInBlock] += partialRank;
      }
      catch (Exception e) {
        System.out.println("" + e + ", on bytesRead: " + bytesRead);
      }
    }

    count = 0;
    for (int i = 0; i < rowIdArray.length; ++i) {
      if (rowIdArray[i] != -1)
        count ++;
    }

    bbuf = ByteBuffer.allocate(count * 12 + 4);
    bbuf.put((byte)0xff);
    bbuf.put((byte)0x00);
    bbuf.put((byte)0xff);
    bbuf.put((byte)0x00);
    for (int i = 0; i < arraySize; ++i) {
      if (rowIdArray[i] != -1) {
        bbuf.putInt(rowIdArray[i]);
        bbuf.putDouble(rankArray[i]);
      }
    }

    System.out.println("map output buffer length: " + bbuf.array().length);
    out.write(bbuf.array());
    in.close();
    end = System.currentTimeMillis();
    System.out.println("Processed edge in " + (end - start) + " ms");
    System.out.println("Edge processing bandwidth: " + bytesRead / (end - start) / 1000 + " MByte/s");
  }
}
