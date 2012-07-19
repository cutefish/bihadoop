package org.apache.hadoop.hdfs.blockcache;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.ipc.VersionedProtocol;

//xyu40@gatech.edu
/******************************************************************************
 * Protocol that a BlockCacheReader used to contact BlockCacheServer.
 * The BlockCacheServer is the server, that implement this protocol.
 */
public interface BlockCacheProtocol extends VersionedProtocol {

  public static final long versionID = 01L;
  public static final int DEFAULT_SERVER_PORT = 60200;

  /**
   * This is a wrapper for the information passed between server and client.
   */
  public static class CachedBlock implements Writable {
    private String fileName;
    private long startOffset;
    private long blockLength;
    private String localPath;

    public CachedBlock() {
      this.fileName = null;
      this.startOffset = 0;
      this.blockLength = 0;
      this.localPath = null;
    }

    public CachedBlock(String fileName, long startOffset,
                       long blockLength, String localPath) {
      this.fileName = fileName;
      this.startOffset = startOffset;
      this.blockLength = blockLength;
      this.localPath = localPath;
    }

    @Override 
    public boolean equals(Object to) {
      if (this == to) return true;
      if (!(to instanceof CachedBlock)) return false;
      return (fileName.equals(((CachedBlock)to).getFileName()) &&
              startOffset == ((CachedBlock)to).getStartOffset());
    }

    @Override
    public int hashCode() {
      return fileName.hashCode() ^ (new Long(startOffset)).hashCode();
    }

    public String getFileName() {
      return fileName;
    }
    
    public long getStartOffset() {
      return startOffset;
    }

    public long getBlockLength() {
      return blockLength;
    }

    public String getLocalPath() {
      return localPath;
    }

    public void setLocalPath(String path) {
      localPath = path;
    }

    //////////////////////////////////////////////////
    // Writable
    //////////////////////////////////////////////////
    static {                                      // register a ctor
      WritableFactories.setFactory
        (CachedBlock.class,
         new WritableFactory() {
           public Writable newInstance() { return new CachedBlock(); }
         });
    }

    public void write(DataOutput out) throws IOException {
      out.writeInt(this.fileName.length());
      out.write(this.fileName.getBytes());
      out.writeLong(this.startOffset);
      out.writeLong(this.blockLength);
      out.writeInt(this.localPath.length());
      out.write(this.localPath.getBytes());
    }

    public void readFields(DataInput in) throws IOException {
      int fileNameLength = in.readInt();
      byte[] fileNameBuf = new byte[fileNameLength];
      in.readFully(fileNameBuf);
      this.fileName = new String(fileNameBuf);
      this.startOffset = in.readLong();
      this.blockLength = in.readLong();
      int localPathLength = in.readInt();
      byte[] localPathBuf = new byte[localPathLength];
      in.readFully(localPathBuf);
      this.localPath = new String(localPathBuf);
    }

  }

  /**
   * Returns the CachedBlock object containing cache information.
   * 
   * The block contains the position required.
   * Throw IOException if cannot cache.
   */
  CachedBlock getCachedBlock(String src, long pos) throws IOException;

  ///**
  // * Heartbeat to keep local file exist.
  // *
  // * Throw IOException if block evicted.
  // */
  //boolean heartbeat(CachedBlock block) throws IOException;
}
