import org.apache.hadoop.ipc.VersionedProtocol;

public interface CacheProtocol extends VersionedProtocol {

  public static final long versionID = 01L;

  String getCachedBlock(String blockId);
}
