/***********************************************************************
    PEGASUS: Peta-Scale Graph Mining System
    Authors: U Kang, Duen Horng Chau, and Christos Faloutsos

This software is licensed under Apache License, Version 2.0 (the  "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-------------------------------------------------------------------------
File: MatvecPrep.java
 - convert matrix(edges) or vectors into block form. 
   This program is used for converting data to be used in the block version of HADI, HCC, and PageRank.
Version: 2.0
***********************************************************************/

package bench.datagen;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Random;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AdjSparseMat extends DataGenerator {

  int rowSize;
  int colSize;
  float sparseLevel;
  URI fsUri;
  Path outPath;

  private void genMatrix() throws Exception {
    File local = new File("/tmp/edge");
    FileOutputStream file = new FileOutputStream(local);
    DataOutputStream out = new DataOutputStream(file);
    Random r = new Random();
    for (int i = 0; i < rowSize; ++i) {
      boolean connected = false;
      for (int j = 0; j < colSize; ++j) {
        if (r.nextFloat() > sparseLevel) continue;
        String line = "" + i + "\t" + j + "\n";
        out.writeBytes(line);
        connected = true;
      }
      if (!connected) {
        int dest = r.nextInt(colSize);
        String line = "" + i + "\t" + dest + "\n";
        out.writeBytes(line);
      }
    }
    FileSystem fs = FileSystem.get(fsUri, conf);
    fs.delete(outPath);
    fs.copyFromLocalFile(false, true, 
                         new Path(local.toString()), outPath);
  }

  public void printUsage() {
    System.out.println("AdjSparseMat <rowSize> <colSize> " + 
                       "<sparseLevel> <fsUri> <outPath>");
  }

  public void parseArgs(String[] args) throws Exception {
    if (args.length != 5) {
      printUsage();
      throw new RuntimeException("invalid argument");
    }
    rowSize = Integer.parseInt(args[0]);
    colSize = Integer.parseInt(args[1]);
    sparseLevel = Float.parseFloat(args[2]);
    fsUri = new URI(args[3]);
    outPath = new Path(args[4]);
  }

  public void generate() throws Exception {
    genMatrix();
  }
}


