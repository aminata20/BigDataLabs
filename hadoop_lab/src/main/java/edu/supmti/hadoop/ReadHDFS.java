package edu.supmti.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {
        // Vérifier les arguments
        if (args.length < 1) {
            System.out.println("Usage: hadoop jar ReadHDFS.jar <file-path>");
            System.out.println("Example: hadoop jar ReadHDFS.jar /user/root/input/purchases.txt");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path nomcomplet = new Path(args[0]);

        // Vérifier l'existence du fichier
        if (!fs.exists(nomcomplet)) {
            System.out.println("File does not exist: " + nomcomplet);
            System.exit(1);
        }

        FSDataInputStream inStream = fs.open(nomcomplet);
        InputStreamReader isr = new InputStreamReader(inStream);
        BufferedReader br = new BufferedReader(isr);
        String line = null;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
        System.out.println(line);
        inStream.close();
        fs.close();
    }
}
