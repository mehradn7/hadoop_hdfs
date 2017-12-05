package hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

public class HdfsStart {

	public static HashMap<String,Integer> hosts = new HashMap<String, Integer>();
	public static Path configFile = Paths.get("src/hdfs/config-serveurs.txt");;
	
	public static void main(String[] args) {
		HdfsStart.setHdfsConfig();
		for(String host : hosts.keySet()) {
			try {
				HdfsServeur.main((new String[] {hosts.get(host).toString()}));
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void setHdfsConfig() {
    	try (BufferedReader reader = Files.newBufferedReader(configFile)) {
    	    String line = null;
    	    while ((line = reader.readLine()) != null) {
    	    	String mots[] = line.split("[ ]+");
    	    	HdfsStart.hosts.put(mots[0],Integer.parseInt(mots[1]));
    	    }
    	} catch (IOException x) {
    	    System.err.format("IOException: %s%n", x);
    	}
    }

}
