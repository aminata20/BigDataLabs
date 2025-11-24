package edu.supmti.hadoop;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
public static void main(String[] args) {
// Vérifier les arguments
if (args.length < 1) {
System.out.println("Usage: hadoop jar HadoopFileStatus.jar <file-path>");
System.out.println("Example: hadoop jar HadoopFileStatus.jar /user/root/input/purchases.txt");
System.exit(1);
}

Configuration conf = new Configuration();
FileSystem fs;
try {
fs = FileSystem.get(conf);
Path filepath = new Path(args[0]);

// Vérifier l'existence AVANT de récupérer le statut
if(!fs.exists(filepath)){
System.out.println("File does not exist: " + filepath);
System.exit(1);
}

FileStatus status = fs.getFileStatus(filepath);
System.out.println(Long.toString(status.getLen())+" bytes");
System.out.println("File Name: "+filepath.getName());
System.out.println("File Size: "+status.getLen());
System.out.println("File owner: "+status.getOwner());
System.out.println("File permission: "+status.getPermission());
System.out.println("File Replication: "+status.getReplication());
System.out.println("File Block Size: "+status.getBlockSize());
BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, 
status.getLen());
            for(BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.println("Block offset: " + blockLocation.getOffset());
                System.out.println("Block length: " + blockLocation.getLength());
                System.out.print("Block hosts: ");
                for (String host : hosts) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }

// Renommer le fichier (optionnel)
// Path newPath = new Path(filepath.getParent(), "achats.txt");
// fs.rename(filepath, newPath);
// System.out.println("File renamed to: " + newPath);
} catch (IOException e) {
// TODO Auto-generated catch block
e.printStackTrace();
}
}
}