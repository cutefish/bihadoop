package bench.datagen;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

public abstract class DataGenerator {
  static protected Configuration conf;
  static {
    conf = new Configuration();
  }

  public void addConfResource(String r) {
    conf.addResource(r);
  }

  abstract public void printUsage();

  abstract public void parseArgs(String[] args) throws Exception;

  abstract public void generate() throws Exception;

  public static void main(String[] args) {
    if (args.length < 1) {
      System.out.println("Available generators:");
      System.out.println("AdjSparseMat\n" + 
                         "");
      System.exit(-1);
    }
    try {
      Class genCls = Class.forName("bench.datagen." + args[0]);
      DataGenerator gener = (DataGenerator) genCls.newInstance();
      gener.parseArgs(Arrays.copyOfRange(args, 1, args.length));
      gener.generate();
    }
    catch (Exception e) {
      System.out.println("Exception: " + StringUtils.stringifyException(e));
    }
  }
}
