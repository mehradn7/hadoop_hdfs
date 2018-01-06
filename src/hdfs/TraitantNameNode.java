package hdfs;

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
		System.out.println(Instant.now());
		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(socket.getInputStream());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ObjectOutputStream oos;
		INode inode;
		System.out.println("NN LA");
		String cmd = "";
		try {
			cmd = (String) ois.readObject();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("NN ICI");

		try {
			switch (cmd) {
			case "connect":
				connect(ois);
				break;
			case "write":
				oos = new ObjectOutputStream(socket.getOutputStream());
				inode = (INode) ois.readObject();
				addINodeToList(inode);
				sendFileMapBlocs(oos, inode);
				break;
			case "read":
				oos = new ObjectOutputStream(socket.getOutputStream());
				inode = (INode) ois.readObject();
				sendFileMapBlocs(oos, inode);
				break;
			case "delete":
				oos = new ObjectOutputStream(socket.getOutputStream());
				inode = (INode) ois.readObject();
				sendFileMapBlocs(oos, inode);
				break;
			case "getServers":
				oos = new ObjectOutputStream(socket.getOutputStream());
				sendAvailableServers(oos);
				break;
			case "getINodes":
				oos = new ObjectOutputStream(socket.getOutputStream());
				sendINodes(oos);
				break;
			default:
				System.out.println("Erreur : commande inconnue");
				break;
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (String host : availableServers.keySet()) {
			System.out.println(host + "-" + availableServers.get(host));
		}
	}
	
	private void addINodeToList(INode inode) {
		HashMap<Integer, ArrayList<String>> repartitionBlocs = HdfsUtil.repartirBlocs(this.availableServers,
				inode.getRepFactor());
		inode.setMapBlocs(repartitionBlocs);
		this.listINodes.add(inode);

		/*
		 * for (INode in : listINodes) { System.out.println(in.getFilename());
		 * for (Integer i : in.getMapBlocs().keySet()) { System.out.println(i +
		 * "->" + in.getMapBlocs().get(i)); } }
		 */

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
				oos.writeObject(in.getMapBlocs());
			}
		}
		if (!fileFound) {
			System.out.println("Fichier non trouvé");
		}
	}

	private void connect(ObjectInputStream ois) throws ClassNotFoundException, IOException {
		String hostname = (String) ois.readObject();
		int port = ois.readInt();
		ois.close();

		this.availableServers.put(hostname, port);

	}


}
