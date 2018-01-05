package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Date;
import java.sql.Time;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class NameNode {
	public static ServerSocket ss;
	public static Socket s;
	public static String hostname = "yoda"; /* TODO : namenode.txt */
	public static int port = 8091;
	public static Map<String, Integer> availableServers = new LinkedHashMap<String, Integer>();
	public static List<INode> listINodes = new ArrayList<INode>();

	public static void main(String[] args) throws IOException, ClassNotFoundException {

		ss = new ServerSocket(port);
		while (true) {
			/* Recevoir une requête */
			s = ss.accept();
			System.out.println(Instant.now());
			ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
			ObjectOutputStream oos;
			INode inode;
			System.out.println("NN LA");
			String cmd = (String) ois.readObject();
			System.out.println("NN ICI");

			switch (cmd) {
			case "connect":
				connect(ois);
				break;
			case "write":
				oos = new ObjectOutputStream(s.getOutputStream());
				inode = (INode) ois.readObject();
				addINodeToList(inode);
				sendFileMapBlocs(oos, inode);
				break;
			case "read":
				oos = new ObjectOutputStream(s.getOutputStream());
				inode = (INode) ois.readObject();
				sendFileMapBlocs(oos, inode);
				break;
			case "delete":
				oos = new ObjectOutputStream(s.getOutputStream());
				inode = (INode) ois.readObject();
				sendFileMapBlocs(oos, inode);
				break;
			case "getServers":
				oos = new ObjectOutputStream(s.getOutputStream());
				sendAvailableServers(oos);
				break;
			case "getINodes":
				oos = new ObjectOutputStream(s.getOutputStream());
				sendINodes(oos);
				break;
			default:
				System.out.println("Erreur : commande inconnue");
				break;
			}

			for (String host : availableServers.keySet()) {
				System.out.println(host + "-" + availableServers.get(host));
			}
		}
	}

	private static void addINodeToList(INode inode) {
		HashMap<Integer, ArrayList<String>> repartitionBlocs = HdfsUtil.repartirBlocs(NameNode.availableServers,
				inode.getRepFactor());
		inode.setMapBlocs(repartitionBlocs);
		NameNode.listINodes.add(inode);

		/*for (INode in : listINodes) {
			System.out.println(in.getFilename());
			for (Integer i : in.getMapBlocs().keySet()) {
				System.out.println(i + "->" + in.getMapBlocs().get(i));
			}
		} */

	}

	private static void sendAvailableServers(ObjectOutputStream oos) throws IOException {
		oos.writeObject(availableServers);
	}

	private static void sendINodes(ObjectOutputStream oos) throws IOException {
		oos.writeObject(listINodes);		
	}

	private static void sendFileMapBlocs(ObjectOutputStream oos, INode inode) throws IOException {
		boolean fileFound = false;
		for (INode in : listINodes) {
			if (in.getFilename().equals(inode.getFilename())) {
				fileFound = true;
				oos.writeObject(in.getMapBlocs());
			}
		}
		if (!fileFound) {
			System.out.println("Fichier non trouvé");
		}
	}

	private static void connect(ObjectInputStream ois) throws ClassNotFoundException, IOException {
		String hostname = (String) ois.readObject();
		int port = ois.readInt();
		ois.close();

		availableServers.put(hostname, port);

	}

}
