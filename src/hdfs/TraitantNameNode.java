package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
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
			ObjectOutputStream oos = null;

			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

			INode inode;
			String cmd = (String) ois.readObject();

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
			case "register":
				System.out.println("Registering...");
				inode = (INode) ois.readObject();
				System.out.println(inode);
				addINodeToList(inode);
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
				removeINodeFromList(inode);
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

			if (!(oos == null)) {
				oos.close();
			}
			socket.close();

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/* Méthode qui supprime un INoe du catalogue de fichiers du NameNode */
	private boolean removeINodeFromList(INode inode) {
		boolean removed = false;
		INode inodeToRemove = null;
		for (INode in : listINodes){
			if (in.getFilename().equals(inode.getFilename())){
				inodeToRemove = in;
			}
		}
		removed = listINodes.remove(inodeToRemove);
		return removed;
	}

	/* Méthode qui ajoute un INode au catalogue de fichiers du NameNode */
	private void addINodeToList(INode inode) {
		/*
		 * Calculer la réparition des blocs de ce INode sur les serveurs, en
		 * fonction des serveurs disponibles
		 */
		HashMap<Integer, ArrayList<String>> repartitionBlocs = HdfsUtil.repartirBlocs(this.availableServers,
				inode.getRepFactor(), inode.getNbOfChunks());

		/* Doter le INode de la répartition de ses morceaux */
		inode.setMapBlocs(repartitionBlocs);

		/* Ajouter le INode au catalogue */
		this.listINodes.add(inode);

	}

	/* Ecrire sur le socket client la liste des serveurs disponibles */
	private void sendAvailableServers(ObjectOutputStream oos) throws IOException {
		oos.writeObject(this.availableServers);
	}

	/* Ecrire sur le socket client le catalogue des INodes */
	private void sendINodes(ObjectOutputStream oos) throws IOException {
		oos.writeObject(this.listINodes);
	}

	/*
	 * Méthode qui recherche un INode dans le catalogue et écrit sur la socket
	 * client la réparition des blocs de ce fichier
	 */
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

	/* Méthode qui gère la connexion d'un serveur HDFS */
	private void connect(ObjectInputStream ois) throws ClassNotFoundException, IOException {
		String hostname = (String) ois.readObject();
		int port = ois.readInt();
		ois.close();

		/* Ajouter le serveur à la liste des serveurs disponibles */
		this.availableServers.put(hostname, port);
		
		System.out.println("Nouvelle connexion de : " + hostname);

	}

}
