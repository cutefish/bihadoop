package org.apache.hadoop.blockcache;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.net.URI;

import org.apache.hadoop.core.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
   * Token to pass the identity of file system and user
   */
  public static class Token {
    final String scheme = null;
    final String authority = null;
    final String userName = null;

    public Token() { }

    Token(URI uri, Configuration conf, String userName) {
      scheme = uri.getScheme();
      if (scheme == null) uri = FileSystem.getDefaultUri(conf);
      authority = uri.getAuthority();
      if (authority == null) authority = "";
      this.userName = userName;
    }

    Token(String scheme, String authority, String userName) {
      this.scheme = scheme;
      this.authority = authority;
      this.userName = userName;
    }

    /** {@inheritDoc} */
    public int hashCode() {
      return toString().hashCode();
    }

    static boolean isEqual(Object a, Object b) {
      return a == b || (a != null && a.equals(b));        
    }

    /** {@inheritDoc} */
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj != null && obj instanceof Key) {
        Key that = (Key)obj;
        return isEqual(this.scheme, that.scheme)
            && isEqual(this.authority, that.authority)
            && isEqual(this.userName, that.userName)
            && isEqual(this.filePath, that.filePath);
      }
      return false;        
    }

    public String getScheme() {
      return scheme;
    }

    public String getAuthority() {
      return authority;
    }

    public String getUserName() {
      return userName;
    }

    /** {@inheritDoc} */
    public String toString() {
      return "("+ userName + ")@" + 
          scheme + "://" + authority + "/" + filePath;        
    }

    //////////////////////////////////////////////////
    // Writable
    //////////////////////////////////////////////////
    static {                                      // register a ctor
      WritableFactories.setFactory
        (Token.class,
         new WritableFactory() {
           public Writable newInstance() { return new Token(); }
         });
    }

    public void write(DataOutput out) throws IOException {
      Text.writeString(out, this.scheme);
      Text.writeString(out, this.authority);
      Text.writeString(out, this.userName);
    }

    public void readFields(DataInput in) throws IOException {
      this.scheme = Text.readString(in);
      this.authority = Text.readString(in);
      this.userName = Text.readString(in);
    }
  }
  /**
   * A wrapper for the information passed between server and client.
   */
  public static class Block implements Writable {
    private String fileName;
    private long startOffset;
    private long blockLength;
    private String localPath;
    private boolean useReplica;

    public Block() {
      this.fileName = "";
      this.startOffset = 0;
      this.blockLength = 0;
      this.localPath = "";
      this.useReplica = false;
    }

    public Block(String fileName, long startOffset,
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
      if (!(to instanceof Block)) return false;
      return (fileName.equals(((Block)to).getFileName()) &&
              startOffset == ((Block)to).getStartOffset());
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
        (Block.class,
         new WritableFactory() {
           public Writable newInstance() { return new Block(); }
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

  /**
   * A writable wrapper for list of blocks
   */
  public static class Blocks implements Writable {

    private Block[] blocks;

    public Blocks() {
    }
    
    public Blocks(Block[] blocks) {
      this.blocks = blocks;
    }

    public Blocks[] get() {
      return blocks;
    }

    //////////////////////////////////////////////////
    // Writable
    //////////////////////////////////////////////////
    static {                                      // register a ctor
      WritableFactories.setFactory
        (Blocks.class,
         new WritableFactory() {
           public Writable newInstance() { return new Blocks(); }
         });
    }

    public void write(DataOutput out) throws IOException {
      out.writeInt(blocks.length);
      for (int i = 0; i < blocks.length; ++i) {
        blocks[i].write(out);
      }
    }

    public void readFields(DataInput in) throws IOException {
      int length = in.readInt();
      blocks[] blocks = new Blocks[length];
      for (int i = 0; i < length; ++i) {
        Block block = new Block();
        block.readFields(in);
        blocks[i] = block;
      }
    }
  }

  ////////////////////////////////////////////////////
  //The Protocol
  ///////////////////////////////////////////////////
  /**
   * Register the client and return a token.
   */
  String registerClient(String userName, 
                        String uri, 
                        Configuration conf) throws IOException;

  /**
   * Unregister a client
   */
  void unregisterClient(String token) throws IOException;

  /**
   * Returns the Block object containing cache information.
   * 
   * The block contains the position required.
   * Throw IOException if cannot cache.
   */
  Block cacheBlockAt(String token, String path, long pos) throws IOException;

  /**
   * Returns a list of blocks cached on the server for a user
   */
  Blocks getCachedBlocks(String user) throws IOException;

}
