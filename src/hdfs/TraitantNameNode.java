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
