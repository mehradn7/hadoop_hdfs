HdfsCommandUnknown.java                                                                             0000640 0115525 0136376 00000000320 13223524140 014772  0                                                                                                    ustar   mrafii                          mrafii                                                                                                                                                                                                                 package hdfs;

public class HdfsCommandUnknown extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String msg;
	
	public String toString() {
		return this.msg;
	}
}
                                                                                                                                                                                                                                                                                                                HdfsFileDoesNotExist.java                                                                           0000640 0115525 0136376 00000000364 13224406275 015245  0                                                                                                    ustar   mrafii                          mrafii                                                                                                                                                                                                                 package hdfs;

public class HdfsFileDoesNotExist extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String msg = "Hdfs file does not exist !!!";
	
	public String toString(){
		return this.msg;
	}
	
}
                                                                                                                                                                                                                                                                            HdfsServeur.java                                                                                    0000640 0115525 0136376 00000006115 13224171423 013502  0                                                                                                    ustar   mrafii                          mrafii                                                                                                                                                                                                                 package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.swing.plaf.synth.SynthSeparatorUI;

import formats.Format;
import formats.KV;
import formats.KvFormat;
import formats.LineFormat;
import ordo.HeartBeatEmitter;

public class HdfsServeur implements Runnable {

	protected String prefix = "hdfs";
	protected int port;
	protected Format file;
	protected Socket s;
	protected String hstname;
	protected String prefixlog;
	private HeartBeatEmitter heartBeatThread;

	/**
	 * Permet de créer un noeud (serveur) hdfs commande : java hdfsserveur port
	 * 
	 * @param args
	 */
	public void run() {
		// port = Integer.parseInt(args[0]);
		this.port = 8090;
		try {
			this.hstname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		/* Signaler la première connexion au NameNode */

		try {
			Socket sn = new Socket(NameNode.hostname, NameNode.port);
			ObjectOutputStream oos = new ObjectOutputStream(sn.getOutputStream());
			oos.writeObject("connect");
			oos.writeObject(hstname);
			oos.writeInt(port);
			oos.close();
			sn.close();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		/* Lancer un thread chargé d'envoyer des HeartBeats au NameNode */
		try {
			this.heartBeatThread = new HeartBeatEmitter(NameNode.hostname, NameNode.heartBeatPort);
			this.heartBeatThread.start(); // .interupt() quand Ctrl C
		} catch (IOException e2) {
			e2.printStackTrace();
		}
		
		/* Attendre les commandes du client */

		ServerSocket ss = null;
		try {
			ss = new ServerSocket(port);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			String[] tmp = Paths.get("").toAbsolutePath().toString().split("/");
			this.prefix = "/" + tmp[1] + "/" + tmp[2] + "/" + this.prefix;
			try {
				Files.createDirectory(Paths.get(this.prefix));
			} catch (FileAlreadyExistsException e) {
			} // besoin de rien besoin de tout
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			this.prefix = this.prefix + "/files-" + this.hstname;
			Files.createDirectory(Paths.get(this.prefix));
		} catch (FileAlreadyExistsException e) {
		} // le fichier est deja cree besoin de rien
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
		this.prefixlog = "[" + this.hstname + ":" + this.port + "] : ";

		System.out.println(this.prefixlog + "Le serveur est lancé.");

		while (true) {
			try {
				System.out.println("LAAA");
				s = ss.accept();
				System.out.println("SERVER READY");
			} catch (IOException e1) {
				e1.printStackTrace();
			} // Bloquante

			new Thread(new TraitantConnexion(s, this.hstname, this.port, this.prefix)).start();

		}
	}
}                                                                                                                                                                                                                                                                                                                                                                                                                                                   HdfsUtil.java                                                                                       0000640 0115525 0136376 00000015076 13224416303 012771  0                                                                                                    ustar   mrafii                          mrafii                                                                                                                                                                                                                 package hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import hdfs.HdfsClient.Commande;

public class HdfsUtil {
	/*
	 * Classe contenant des méthodes statiques utilisées par les autres classes
	 * de l'application
	 */

	/*
	 * Méthode statique destinée à être appelée hors HDFS Cette méthode ouvre
	 * une socket sur le NameNode et lui envoie une requête d'accès à la liste
	 * des serveurs disponibles
	 */
	public static HashMap<String, Integer> getAvailableServers()
			throws UnknownHostException, IOException, ClassNotFoundException {
		Socket s = new Socket(NameNode.hostname, NameNode.port);
		ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
		oos.writeObject("getServers");
		ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
		@SuppressWarnings("unchecked")
		HashMap<String, Integer> res = (HashMap<String, Integer>) ois.readObject();
		s.close();
		return res;
	}

	public static ArrayList<INode> getListINodes() throws UnknownHostException, IOException, ClassNotFoundException {
		Socket s = new Socket(NameNode.hostname, NameNode.port);
		ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
		oos.writeObject("getINodes");
		ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
		@SuppressWarnings("unchecked")
		ArrayList<INode> res = (ArrayList<INode>) ois.readObject();
		s.close();
		return res;
	}

	/*
	 * Méthode statique appelée depuis HdfsClient lors d'une requête d'écriture
	 */
	@SuppressWarnings("unchecked")
	public static HashMap<Integer, ArrayList<String>> getStrategieRepartition(INode inode, Commande cmd)
			throws UnknownHostException, IOException, ClassNotFoundException {
		Socket s = new Socket(NameNode.hostname, NameNode.port);
		HashMap<Integer, ArrayList<String>> repartitionBlocs = new HashMap<Integer, ArrayList<String>>();
		ObjectInputStream ois;
		ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
		switch (cmd) {
		case CMD_READ:
			oos.writeObject("read");
			oos.writeObject(inode);
			ois = new ObjectInputStream(s.getInputStream());
			repartitionBlocs = (HashMap<Integer, ArrayList<String>>) ois.readObject();
			ois.close();
			break;
		case CMD_WRITE:
			oos.writeObject("write");
			oos.writeObject(inode);
			ois = new ObjectInputStream(s.getInputStream());
			repartitionBlocs = (HashMap<Integer, ArrayList<String>>) ois.readObject();
			ois.close();
			break;
		case CMD_DELETE:
			oos.writeObject("delete");
			oos.writeObject(inode);
			ois = new ObjectInputStream(s.getInputStream());
			repartitionBlocs = (HashMap<Integer, ArrayList<String>>) ois.readObject();
			ois.close();

			break;
		default:
			System.out.println("Erreur : commande inconnue ");
			break;
		}

		oos.close();
		s.close();

		for (Integer i : repartitionBlocs.keySet()) {
			for (String server : repartitionBlocs.get(i)) {
				System.out.println(i + "->" + server);

			}
		}
		return repartitionBlocs;

	}

	/*
	 * Méthode statique qui renvoie une répartition des blocs d'un fichier en
	 * fonction des serveurs disponibles et du facteur de réplication. Cette
	 * méthode est une boîte noire qui peut être vue comme indépendante de HDFS
	 * PRECONDITION : repFactor <= nbServers
	 */
	public static HashMap<Integer, ArrayList<String>> repartirBlocs(Map<String, Integer> availableServers,
			int repFactor, int nbFragment) {

		HashMap<Integer, ArrayList<String>> repartitionBlocs = new HashMap<Integer, ArrayList<String>>();

		// liste contenant le nom des serveurs disponibles
		ArrayList<String> listServers = new ArrayList<String>(availableServers.keySet());

		// Nombre de serveurs disponibles
		int nbServers = availableServers.size();

		// On dispose tout d'abord au moins un fragment du fichier sur chacun
		// des
		// serveurs disponibles
		for (int i = 1; i < nbFragment + 1; i++) {
			int j;
			j = i % nbServers;
			ArrayList<String> listeServeurs = new ArrayList<String>();
			listeServeurs.add(listServers.get(j));
			repartitionBlocs.put(i, listeServeurs);
		}
		if (repFactor > 1) {
			Random rand = new Random();
			int randNumber;
			// On considère chaque fragment
			for (int i = 1; i < nbFragment + 1; i++) {

				// On recrée la liste des serveurs disponibles
				listServers = new ArrayList<String>(availableServers.keySet());
				listServers.remove(i % nbServers);

				// Et on le duplique autant de fois que nécessaire
				for (int j = 0; j < (repFactor - 1); j++) {

					// Nombre de serveurs sur lequels on peut dupliquer le
					// fragment
					int nbS = listServers.size();
					// On récupère la liste des serveurs associée à ce fragment
					ArrayList<String> listeServeurFragment = repartitionBlocs.get(i);

					randNumber = rand.nextInt(nbS);

					listeServeurFragment.add(listServers.get(randNumber));
					// On enlève le serveur choisi de la liste des serveurs
					// disponibles pour ne pas écrire plusieurs fois le même
					// fragment sur celui-ci
					listServers.remove(randNumber);
				}
			}
		}

		return repartitionBlocs;
	}

	/*
	 * Méthode qui découpe un fichier LIGNE PAR LIGNE en morceaux de taille
	 * chunkSize (en KB) path : chemin d'accès du fichier chunkSize : la taille
	 * désirée de chaque morceau (en KB)
	 *
	 * retour : le nombre de morceaux après le découpage
	 *
	 * TESTS : OK
	 */
	public static int splitFile(String path, int chunkSize) throws IOException {
		FileReader fileReader = new FileReader(path);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line = "";
		int fileSize = 0;
		int chunkNumber = 1;
		BufferedWriter fos = new BufferedWriter(new FileWriter(path + chunkNumber, true));
		while ((line = bufferedReader.readLine()) != null) {
			if (fileSize + line.getBytes().length > chunkSize * 1024) {
				fos.close();
				fos = new BufferedWriter(new FileWriter(path + (++chunkNumber), true));
				fos.write(line);
				fos.newLine();
				fileSize = line.getBytes().length;
			} else {
				fos.write(line);
				fos.newLine();
				fileSize += line.getBytes().length;
			}
		}
		fos.flush();
		fos.close();
		bufferedReader.close();

		return chunkNumber;

	}

	public static void printHashMap(HashMap<Integer, ArrayList<String>> hmap) {
		for (Integer i : hmap.keySet()) {
			for (String server : hmap.get(i)) {
				System.out.println(i + "->" + server);

			}
		}		
	}

}
                                                                                                                                                                                                                                                                                                                                                                                                                                                                  HeartBeatReceiverNameNode.java                                                                      0000640 0115525 0136376 00000004243 13224201624 016172  0                                                                                                    ustar   mrafii                          mrafii                                                                                                                                                                                                                 package hdfs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;


import ordo.IHeartBeatReceiver;

public class HeartBeatReceiverNameNode extends Thread implements IHeartBeatReceiver {
	
	private ServerSocket ss;
	private Map<String, Integer> availableServers;
	private HashMap<String, Socket> sockets;
	private Iterator<Socket> it_sockets;
	
	public HeartBeatReceiverNameNode(Map<String, Integer> availableServers) throws IOException {
		super();
		this.ss = new ServerSocket(NameNode.heartBeatPort);
		this.ss.setSoTimeout(300);
		this.sockets = new HashMap<String, Socket>();
		this.availableServers = availableServers;
	}

	@Override
	public void run() {
		/*
		 * Itérateur car modification concurrente de la liste en parallèle du parcours.
		 */
		while(true) {
			try {
				this.addEmitter(this.ss.accept());
				System.out.println("1 reçu");
			} catch (SocketTimeoutException e2) {//timeout
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			this.it_sockets = this.sockets.values().iterator();
			Socket s;
			while(it_sockets.hasNext()) {
				s = it_sockets.next();
				try {
					if (s.getInputStream().read() != 1) {
						this.removeEmitter(s);
					}
				} catch (IOException e) {
					this.removeEmitter(s);
				}
			}
		}
	}

	@Override
	synchronized public void addEmitter(Socket s) {
		this.sockets.put(s.getInetAddress().getHostName().replaceFirst(".enseeiht.fr", ""), s);
		
	}

	@Override
	synchronized public void removeEmitter(Socket s) {
		String hostname = s.getInetAddress().getHostName().replaceFirst(".enseeiht.fr", "");
		System.out.println("SERVER REMOVED : " + hostname);
		System.out.println(this.availableServers.remove(hostname));
		this.it_sockets.remove();
		System.out.println(this.availableServers.size());
		for (String host : availableServers.keySet()) {
			System.out.println(host + "-" + availableServers.get(host));
		}
		try {
			s.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}

}
                                                                                                                                                                                                                                                                                                                                                             INode.java                                                                                          0000640 0115525 0136376 00000002561 13223524140 012236  0                                                                                                    ustar   mrafii                          mrafii                                                                                                                                                                                                                 package hdfs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class INode implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String filename;
	private int repFactor;
	private int nbOfChunks;
	private HashMap<Integer, ArrayList<String>> mapBlocs;

	public INode(String filename){
		this.filename = filename;
	}
	public INode(String filename, int repFactor, int nbOfChunks) {
		this.filename = filename;
		this.repFactor = repFactor;
		this.setNbOfChunks(nbOfChunks);
		this.mapBlocs = new HashMap<Integer, ArrayList<String>>();
	}
	
	public INode(String filename, int repFactor, HashMap<Integer, ArrayList<String>> mapBlocs) {
		this.filename = filename;
		this.repFactor = repFactor;
		this.mapBlocs = mapBlocs;
	}

	public int getRepFactor() {
		return repFactor;
	}

	public void setRepFactor(int repFactor) {
		this.repFactor = repFactor;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public HashMap<Integer, ArrayList<String>> getMapBlocs() {
		return mapBlocs;
	}
	
	public void setMapBlocs(HashMap<Integer, ArrayList<String>> mapBlocs) {
		this.mapBlocs = mapBlocs;
	}
	public int getNbOfChunks() {
		return nbOfChunks;
	}
	public void setNbOfChunks(int nbOfChunks) {
		this.nbOfChunks = nbOfChunks;
	}

}
                                                                                                                                               NameNode.java                                                                                       0000640 0115525 0136376 00000002512 13224413577 012736  0                                                                                                    ustar   mrafii                          mrafii                                                                                                                                                                                                                 package hdfs;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class NameNode implements Runnable {
	public static String hostname = "bobafett";
	public static int port = 8091;
	public static int heartBeatPort = 5002;
	private ServerSocket ss;
	private Socket s;
	private Map<String, Integer> availableServers = new LinkedHashMap<String, Integer>();
	private List<INode> listINodes = new ArrayList<INode>();

	@Override
	public void run() {

		/* Lancer un thread chargé de recevoir les heartBeats des serveurs */
		runHeartBeatReceiver();

		/* Ouvrir le ServerSocket */
		openServerSocket(port);

		while (true) {
			/* Recevoir une requête */
			try {
				s = ss.accept();
			} catch (IOException e) {
				e.printStackTrace();
			}

			/* Lancer un thread qui va traiter la requête */
			new Thread(new TraitantNameNode(s, this.availableServers, this.listINodes)).start();

		}
	}

	private void runHeartBeatReceiver() {
		try {
			new HeartBeatReceiverNameNode(this.availableServers).start();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	private void openServerSocket(int port) {
		try {
			ss = new ServerSocket(port);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
                                                                                                                                                                                      RunHdfsServeur.java                                                                                 0000640 0115525 0136376 00000000230 13224413564 014164  0                                                                                                    ustar   mrafii                          mrafii                                                                                                                                                                                                                 package hdfs;

public class RunHdfsServeur {

	public static void main(String[] args) {
		HdfsServeur server = new HdfsServeur();
		server.run();
	}

}
                                                                                                                                                                                                                                                                                                                                                                        RunNameNode.java                                                                                    0000640 0115525 0136376 00000000541 13224407063 013414  0                                                                                                    ustar   mrafii                          mrafii                                                                                                                                                                                                                 package hdfs;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class RunNameNode {

	public static void main(String[] args) {
		NameNode namenode = new NameNode();
		try {
			NameNode.hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		namenode.run();
	}

}
                                                                                                                                                               SlaveHdfsClientDelete.java                                                                          0000640 0115525 0136376 00000001255 13223770167 015414  0                                                                                                    ustar   mrafii                          mrafii                                                                                                                                                                                                                 package hdfs;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Path;

public class SlaveHdfsClientDelete extends Thread {
	
	private Socket s;
	private String hdfsFname;
	
	public SlaveHdfsClientDelete(String host, int port, String hdfsFname) 
			throws UnknownHostException, IOException {
		this.s = new Socket(host, port);
		this.hdfsFname = hdfsFname;
	}
	
	public void run(){
		try{
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			oos.writeObject("delete");
			oos.writeObject(this.hdfsFname);
		}catch(IOException e1) {
			e1.printStackTrace();
		}
	}
}
                                                                                                                                                                                                                                                                                                                                                   SlaveHdfsClientRead.java                                                                            0000640 0115525 0136376 00000001650 13223524140 015050  0                                                                                                    ustar   mrafii                          mrafii                                                                                                                                                                                                                 package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

public class SlaveHdfsClientRead extends Thread {
	
	private Socket s;
	private String hdfsFname;
	private ObjectInputStream ois;

	public SlaveHdfsClientRead(String host, int port, String hdfsFname) 
			throws UnknownHostException, IOException {
		this.s = new Socket(host, port);
		this.hdfsFname = hdfsFname;
	}
	
	public void run(){
		try{
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			oos.writeObject("read");
			oos.writeObject(this.hdfsFname);
			setObjectInputStream(new ObjectInputStream(s.getInputStream()));
		}catch(IOException e1) {
			e1.printStackTrace();
		}
	}

	public ObjectInputStream getObjectInputStream() {
		return ois;
	}

	public void setObjectInputStream(ObjectInputStream ois) {
		this.ois = ois;
	}
}

                                                                                        SlaveHdfsClientWrite.java                                                                           0000640 0115525 0136376 00000003706 13224151173 015276  0                                                                                                    ustar   mrafii                          mrafii                                                                                                                                                                                                                 package hdfs;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;

import formats.Format;
import formats.Format.OpenMode;
import formats.KV;
import formats.KvFormat;
import formats.LineFormat;

public class SlaveHdfsClientWrite extends Thread {

	private Socket s;
	private Format.Type fileType;
	private Format file;
	private String host;
	private int port;
	private String fname;
	private int idBloc;

	public SlaveHdfsClientWrite(String host, int port, String localFSSourceFname,
			Format.Type fmt, int idBloc) throws UnknownHostException, IOException {
		this.host = host;
		this.port = port;
		this.s = new Socket(host, port);
		this.fileType = fmt;
		this.fname = localFSSourceFname;
		this.idBloc = idBloc;
		String pathString = "../data/" + localFSSourceFname + this.idBloc;
		switch (fmt) {
		case LINE:
			this.file = (Format) new LineFormat(pathString);
			break;
		case KV:
			this.file = (Format) new KvFormat(pathString);
			break;
		default:
			System.out.println("Format inconnu !!!");
		}
	}

	public void run() {
		// System.out.println("Écriture commencée...");

		// 1) Découper le fichier localement en morceaux de taille fixe (faire
		// ça dans HdfsClient write)

		// 2)Ecrire PARALLELEMENT chaque morceau sur un serveur (selon la
		// stratégie de
		// répartition qui sera en attribut de la classe, i.e. une HashMap
		file.open(OpenMode.R);
		try {
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			oos.writeObject("write");
			oos.writeObject(fileType.toString());
			oos.writeObject(this.fname + this.idBloc);
			KV kv;
			while ((kv = file.read()) != null) {
				oos.writeObject(kv);
			}
			oos.writeObject(null);
			s.close();
			// System.out.println("Écriture terminée !");
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// 3) Supprimer les morceaux de fichier (faire ça dans HdfsClient write)
	}
}
                                                          TestRepartirBlocs.java                                                                              0000640 0115525 0136376 00000001252 13224204230 014643  0                                                                                                    ustar   mrafii                          mrafii                                                                                                                                                                                                                 package hdfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class TestRepartirBlocs {

	public static void main(String[] args) {
		LinkedHashMap<String, Integer> availableServers = new LinkedHashMap<String, Integer>();
		availableServers.put("bore", 8090);
		availableServers.put("carbone", 8090);
		availableServers.put("luke", 8090);
		
		HashMap<Integer, ArrayList<String>> repartition = HdfsUtil.repartirBlocs(availableServers,
				2, 5);
		
		/* afficher la hashmap résultat */
		for (Integer i : repartition.keySet()) {
			for (String server : repartition.get(i)) {
				System.out.println(i + "->" + server);

			}
		}

	}

}
                                                                                                                                                                                                                                                                                                                                                      TestSplitFile.java                                                                                  0000640 0115525 0136376 00000000344 13223524140 013770  0                                                                                                    ustar   mrafii                          mrafii                                                                                                                                                                                                                 package hdfs;

import java.io.IOException;

public class TestSplitFile {

	public static void main(String[] args) {
		try {
			HdfsUtil.splitFile("testfichier", 10);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
                                                                                                                                                                                                                                                                                            TraitantConnexion.java                                                                              0000640 0115525 0136376 00000007541 13224151726 014721  0                                                                                                    ustar   mrafii                          mrafii                                                                                                                                                                                                                 package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;

import formats.Format;
import formats.KV;
import formats.KvFormat;
import formats.LineFormat;

public class TraitantConnexion implements Runnable {

	protected Socket socket;
	protected String hstname;
	protected int port;
	protected String prefixlog = "";
	protected String cmd = "";
	protected String prefix = "";

	public TraitantConnexion(Socket s, String hostname, int port, String prefixPath) {
		this.socket = s;
		this.hstname = hostname;
		this.port = port;
		this.prefixlog = "[" + this.hstname + ":" + this.port + "] : ";
		this.prefix = prefixPath;
	}

	@Override
	public void run() {
		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(socket.getInputStream());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println(this.prefixlog + "Le serveur accepte une nouvelle connexion.");
		try {

			cmd = (String) ois.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(this.prefixlog + "Action demandée : " + cmd);
		switch (cmd) {
		case "write":
			// ce genre de try sert à éviter de faire crash le serveur par
			// une mauvaise manip du client
			try {
				write(ois);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(this.prefixlog + "Erreur...");
			}
			break;
		case "delete":
			try {
				delete(ois);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(this.prefixlog + "Erreur...");
			}
			break;
		case "read":
			try {
				read(ois);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(this.prefixlog + "Erreur...");
			}
			break;
		default:
			System.out.println(this.prefixlog + "Erreur sur la commande...");
			break;
		}
		try {
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void write(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		KV res;
		String fileType = (String) ois.readObject();
		String fileName = (String) ois.readObject();
		Format fmt = this.getFormat(fileType, fileName);
		fmt.open(Format.OpenMode.W);
		System.out.println(this.prefixlog + "Writing : " + fileName);
		while ((res = (KV) ois.readObject()) != null) {
			fmt.write(res);
		}
		fmt.close();
		System.out.println(this.prefixlog + "Le serveur a fini d'écrire : " + fileName);
	}

	public void read(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		String fileName = (String) ois.readObject();
		System.out.println(this.prefixlog + "Lecture de : " + fileName);
		Format fmt = (Format) new LineFormat(this.prefix + "/" + fileName);
		fmt.open(Format.OpenMode.R);
		KV res;
		ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
		while ((res = fmt.read()) != null) {
			oos.writeObject(res);
		}
		fmt.close();
		oos.writeObject(null);
		System.out.println(this.prefixlog + "Le serveur a fini de lire : " + fileName);
	}

	public void delete(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		String fileName = (String) ois.readObject();
		try {
			Files.delete(Paths.get(this.prefix + "/" + fileName));
			System.out.println(this.prefixlog + "suppression du fichier -> " + fileName);
		} catch (java.nio.file.NoSuchFileException e) {
			System.out.println(this.prefixlog + "fichier introuvable : " + fileName);
		}
	}

	public Format getFormat(String fileType, String fileName) {
		Format file;
		String filePath = this.prefix + "/" + fileName;
		switch (fileType) {
		case "LINE":
			file = (Format) new LineFormat(filePath);
			break;
		case "KV":
			file = (Format) new KvFormat(filePath);
			break;
		default:
			file = null;
			System.out.println(this.prefixlog + "Format non reconnu !!!");
		}
		return file;
	}

}
                                                                                                                                                               TraitantNameNode.java                                                                               0000640 0115525 0136376 00000006175 13224414726 014453  0                                                                                                    ustar   mrafii                          mrafii                                                                                                                                                                                                                 package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TraitantNameNode implements Runnable {

	private Socket socket;
	private Map<String, Integer> availableServers;
	private List<INode> listINodes;

	public TraitantNameNode(Socket s, Map<String, Integer> availableServers, List<INode> listINodes) {
		this.socket = s;
		this.availableServers = availableServers;
		this.listINodes = listINodes;
	}

	@Override
	public void run() {

		try {
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());

			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
			
			INode inode;
			String cmd = (String) ois.readObject();

			switch (cmd) {
			case "connect":
				connect(ois);
				break;
			case "write":
				//oos = new ObjectOutputStream(socket.getOutputStream());
				inode = (INode) ois.readObject();
				addINodeToList(inode);
				sendFileMapBlocs(oos, inode);
				break;
			case "read":
				//oos = new ObjectOutputStream(socket.getOutputStream());
				inode = (INode) ois.readObject();
				sendFileMapBlocs(oos, inode);
				break;
			case "delete":
				//oos = new ObjectOutputStream(socket.getOutputStream());
				inode = (INode) ois.readObject();
				sendFileMapBlocs(oos, inode);
				break;
			case "getServers":
				//oos = new ObjectOutputStream(socket.getOutputStream());
				sendAvailableServers(oos);
				break;
			case "getINodes":
				//oos = new ObjectOutputStream(socket.getOutputStream());
				sendINodes(oos);
				break;
			default:
				System.out.println("Erreur : commande inconnue");
				break;
			}

			if (!(oos == null)) {
				oos.close();
			}
			ois.close();
			socket.close();
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void addINodeToList(INode inode) {
		HashMap<Integer, ArrayList<String>> repartitionBlocs = HdfsUtil.repartirBlocs(this.availableServers,
				inode.getRepFactor(), inode.getNbOfChunks());
		inode.setMapBlocs(repartitionBlocs);
		this.listINodes.add(inode);


	}

	private void sendAvailableServers(ObjectOutputStream oos) throws IOException {
		oos.writeObject(this.availableServers);
	}

	private void sendINodes(ObjectOutputStream oos) throws IOException {
		oos.writeObject(this.listINodes);
	}

	private void sendFileMapBlocs(ObjectOutputStream oos, INode inode) throws IOException {
		boolean fileFound = false;
		for (INode in : this.listINodes) {
			if (in.getFilename().equals(inode.getFilename())) {
				fileFound = true;
				HashMap<Integer, ArrayList<String>> mapBlocs = new HashMap<Integer, ArrayList<String>>();
				mapBlocs = in.getMapBlocs();
				oos.writeObject(mapBlocs);
			}
		}
		if (!fileFound) {
			throw new RuntimeException("Fichier non trouvé");
		}
	}

	private void connect(ObjectInputStream ois) throws ClassNotFoundException, IOException {
		String hostname = (String) ois.readObject();
		int port = ois.readInt();
		this.availableServers.put(hostname, port);

	}

}
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   