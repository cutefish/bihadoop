/* FakeFileNameSystem.java
 */

public class FakeFileNameSystem {
  int numDataNode = 50;
  int numReplica = 3;
  Map<Path, Block[]> fileBlocks;
  Map<Block, Node[]> blockLocations;

  public void createFile(Path p) {
  }

  public Block[] getFileBlocks(Path p) {
  }

  public Node[] getBlockLocations(Block b) {
  }
}
