package hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
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
		return res;
	}

	public static ArrayList<INode> getListINodes() throws UnknownHostException, IOException, ClassNotFoundException {
		Socket s = new Socket(NameNode.hostname, NameNode.port);
		ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
		oos.writeObject("getINodes");
		ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
		@SuppressWarnings("unchecked")
		ArrayList<INode> res = (ArrayList<INode>) ois.readObject();
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
			break;
		case CMD_WRITE:
			oos.writeObject("write");
			oos.writeObject(inode);
			ois = new ObjectInputStream(s.getInputStream());
			repartitionBlocs = (HashMap<Integer, ArrayList<String>>) ois.readObject();
			break;
		case CMD_DELETE:
			oos.writeObject("delete");
			oos.writeObject(inode);
			ois = new ObjectInputStream(s.getInputStream());
			repartitionBlocs = (HashMap<Integer, ArrayList<String>>) ois.readObject();
			break;
		default:
			System.out.println("Erreur : commande inconnue ");
			break;
		}

		/* Déterminer la répartition de chaque bloc sur les serveurs HDFS */
		return repartitionBlocs;

	}

	/*
	 * Méthode statique qui renvoie une répartition des blocs d'un fichier en
	 * fonction des serveurs disponibles et du facteur de réplication. Cette
	 * méthode est une boîte noire qui peut être vue comme indépendante de HDFS
	 */
	public static HashMap<Integer, ArrayList<String>> repartirBlocs(Map<String, Integer> availableServers,
			int repFactor) {
		HashMap<Integer, ArrayList<String>> repartitionBlocs = new HashMap<Integer, ArrayList<String>>();

		// liste contenant le nom des serveurs disponibles
		ArrayList<String> listServers = new ArrayList<String>(availableServers.keySet());

		// Nombre de serveurs disponibles
		int nbServers = availableServers.size();

		// On dispose tout d'abord un fragment du fichier sur chacun des
		// serveurs disponibles
		for (int i = 0; i < nbServers; i++) {
			ArrayList<String> listeServeurs = new ArrayList<String>();
			listeServeurs.add(listServers.get(i));
			repartitionBlocs.put(i, listeServeurs);
		}
		if (repFactor > 1) {
			Random rand = new Random();
			int randNumber;
			// On considère chaque fragment
			for (int i = 0; i < nbServers; i++) {

				// On recrée la liste des serveurs disponibles
				listServers = new ArrayList<String>(availableServers.keySet());
				listServers.remove(i);

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
	 * chunkSize (en KB) 
	 * path : chemin d'accès du fichier 
	 * chunkSize : la taille désirée de chaque morceau (en KB)
	 * 
	 * retour : le nombre de morceaux après le découpage
	 * 
	 * On obtient à chaque fois le 1er morceau de taille 20 KB (pas grave)
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
				fos = new BufferedWriter(new FileWriter(path + (chunkNumber++), true));
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

}
