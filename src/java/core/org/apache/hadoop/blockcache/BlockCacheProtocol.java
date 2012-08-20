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
 * Protocol that a BlockCacheClient used to contact BlockCacheServer.
 * The BlockCacheServer is the server, that implement this protocol.
 ******************************************************************************/
public interface BlockCacheProtocol extends VersionedProtocol {

  public static final long versionID = 01L;
  public static final int DEFAULT_SERVER_PORT = 60200;

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
  Token registerFS(String userName, 
                       String uri, 
                       Configuration conf) throws IOException;

  /**
   * Returns the Block object containing cache information.
   * 
   * The block contains the position required.
   * Throw IOException if cannot cache.
   */
  Block cacheBlockAt(Token token, String path, long pos) throws IOException;

  /**
   * Returns a list of blocks cached on the server for a user
   */
  Blocks getCachedBlocks(String user) throws IOException;

}
