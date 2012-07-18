package org.apache.hadoop.hdfs.blockcache;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

//xyu40@gatech.edu
/******************************************************************************
 * Protocol that a BlockCacheReader used to contact BlockCacheServer.
 * The BlockCacheServer is the server, that implement this protocol.
 */
public interface BlockCacheProtocol extends VersionedProtocol {

  public static final long versionID = 01L;
  public static final int DEFAULT_SERVER_PORT = 50200;

  /**
   * This is a wrapper for the information passed between server and client.
   */
  public static class CachedBlock {
    private String fileName;
    private long startOffset;
    private long blockLength;
    private String localPath;

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
