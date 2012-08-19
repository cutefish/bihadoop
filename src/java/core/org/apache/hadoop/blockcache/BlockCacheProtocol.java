package org.apache.hadoop.blockcache;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.net.URI;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.ipc.VersionedProtocol;

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
  public static class RequestBlock implements Writable {
    private String fileName;
    private long startOffset;
    private long blockLength;
    private String localPath;
    private boolean useReplica;

    public RequestBlock() {
      this.fileName = "";
      this.startOffset = 0;
      this.blockLength = 0;
      this.localPath = "";
      this.useReplica = false;
    }

    public RequestBlock(String fileName, long startOffset,
                       long blockLength, String localPath, 
                       boolean useReplica) {
      this.fileName = fileName;
      this.startOffset = startOffset;
      this.blockLength = blockLength;
      this.localPath = localPath;
    }

    @Override 
    public boolean equals(Object to) {
      if (this == to) return true;
      if (!(to instanceof RequestBlock)) return false;
      return (fileName.equals(((RequestBlock)to).getFileName()) &&
              startOffset == ((RequestBlock)to).getStartOffset());
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

    public boolean shouldUseReplica() {
      return useReplica;
    }

    //////////////////////////////////////////////////
    // Writable
    //////////////////////////////////////////////////
    static {                                      // register a ctor
      WritableFactories.setFactory
        (RequestBlock.class,
         new WritableFactory() {
           public Writable newInstance() { return new RequestBlock(); }
         });
    }

    public void write(DataOutput out) throws IOException {
      Text.writeString(out, this.fileName);
      out.writeLong(this.startOffset);
      out.writeLong(this.blockLength);
      Text.writeString(out, this.localPath);
      out.write(this.localPath.getBytes());
    }

    public void readFields(DataInput in) throws IOException {
      this.fileName = Text.readString(in);
      this.startOffset = in.readLong();
      this.blockLength = in.readLong();
      this.localPath = Text.readString(in);
    }

  }

  ////////////////////////////////////////////////////
  //The Protocol
  ///////////////////////////////////////////////////
  /**
   * Register the client and return a token.
   */
  String registerClient(String userName, String uri, Configuration conf);

  /**
   * Unregister a client
   */
  void unregisterClient(String token);

  /**
   * Returns the RequestBlock object containing cache information.
   * 
   * The block contains the position required.
   * Throw IOException if cannot cache.
   */
  RequestBlock cacheBlockAt(String token, String path, long pos) throws IOException;

}
