/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import formats.Format;
import formats.KV;
import formats.LineFormat;

public class HdfsClient {
	
	private static Path configFile = Paths.get("../src/config/config-serveurs.txt"); /* EN RELATIF PAR RAPPORT A BIN*/
	private static ArrayList<String> hl = new ArrayList<String>(); // Adresses des serveurs
	private static ArrayList<Integer> pl = new ArrayList<Integer>(); // Ports des serveurs
	public static enum Commande {CMD_READ, CMD_WRITE, CMD_DELETE};

    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }
	
    public static void HdfsDelete(String hdfsFname) throws UnknownHostException, IOException {
    	SlaveHdfsClientDelete sl;
    	for (int i = 0; i < hl.size(); i++) {
    		sl = new SlaveHdfsClientDelete(hl.get(i), pl.get(i), hdfsFname);
    		sl.start();
    	}
    	System.out.println("Fichier effacé du service Hdfs.");
    }
	
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, 
     int repFactor) throws UnknownHostException, IOException, InterruptedException {
    	Path p = Paths.get("../data/"+localFSSourceFname);
		long nbOfLines = Files.lines(p).count();
    	int chunkSize = (int) nbOfLines / hl.size() + 1;
    	SlaveHdfsClientWrite sl;
    	for (int i = 0; i < hl.size(); i++) {
    		sl = new SlaveHdfsClientWrite(hl.get(i), pl.get(i), (i*chunkSize)+1, chunkSize,
    				localFSSourceFname, fmt);
    		sl.start();
    	}
    }

    public static void HdfsRead(String hdfsFname, String localFSDestFname) throws UnknownHostException,
    	IOException, ClassNotFoundException, InterruptedException {
    	System.out.println("Lecture en cours ...");
    	ArrayList<SlaveHdfsClientRead> sl = new ArrayList<SlaveHdfsClientRead>();
    	SlaveHdfsClientRead currentSlave;
    	for (int i = 0; i < hl.size(); i++) {
    		currentSlave  = new SlaveHdfsClientRead(hl.get(i), pl.get(i), hdfsFname);
    		sl.add(currentSlave);
    		currentSlave.start();
    	}
    	Format file = (Format) new LineFormat("../data/"+localFSDestFname);
    	file.open(Format.OpenMode.W);
    	ObjectInputStream ois;
    	KV res;
    	for (int i = 0; i < hl.size(); i++) {
    		sl.get(i).join();
        	ois = sl.get(i).getObjectInputStream();
        	while ((res = (KV) ois.readObject()) != null) {
        		file.write(res);
    		}
    		ois.close();
    	}
    	//System.out.println("Lecture terminée ! ");
    }
    
    public static void setHdfsConfig() {
    	try (BufferedReader reader = Files.newBufferedReader(configFile)) {
    	    String line;
    	    while ((line = reader.readLine()) != null) {
    	    	String mots[] = line.split("[ ]+");
    	    	hl.add(mots[0]);
    	    	pl.add(Integer.parseInt(mots[1]));
    	    }
    	} catch (IOException x) {
    	    System.err.format("IOException: %s%n", x);
    	}
    }

	
    public static void main(String[] args) {
        // java HdfsClient <read|write> <line|kv> <file>
    	
    	HdfsClient.setHdfsConfig();

        try {
            if (args.length<2) {usage(); return;}

            switch (args[0]) {
              case "read": 
            	  HdfsRead(args[1],args[1]);
            	  break;
              case "delete": 
            	  HdfsDelete(args[1]); 
            	  break;
              case "write": 
                Format.Type fmt;
                if (args.length < 3) {
                	usage(); 
                	return;
                }
                if (args[1].equals("line")) fmt = Format.Type.LINE;
                else if(args[1].equals("kv")) fmt = Format.Type.KV;
                else {
                	usage(); 
                	return;
                	}
                HdfsWrite(fmt,args[2],1);
            }	
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
