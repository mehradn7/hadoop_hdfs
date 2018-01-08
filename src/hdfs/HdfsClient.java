package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;

import formats.Format;
import formats.Format.Type;
import formats.KV;
import formats.LineFormat;

public class HdfsClient {
	public static enum Commande {
		CMD_READ, CMD_WRITE, CMD_DELETE
	};

	private static void usage() {
		System.out.println("Usage: java HdfsClient read <fileSourceName> (<fileDestName>)");
		System.out.println("Usage: java HdfsClient write <line|kv> <file> (<repFactor>)");
		System.out.println("Usage: java HdfsClient delete <file>");
	}

	public static void HdfsDelete(String hdfsFname) throws UnknownHostException, IOException, ClassNotFoundException {
		INode fileNode = new INode(hdfsFname);

		/* Quitter si le fichier demandé n'existe pas */
		if (!(checkCatalog(hdfsFname))) {
			System.out.println("Ce fichier n'existe pas.");
			return;
		}

		/* Récupérer auprès du NameNode la répartition des blocs de fichier */
		HashMap<Integer, ArrayList<String>> repBlocs = HdfsUtil.getStrategieRepartition(fileNode, Commande.CMD_DELETE);

		/*
		 * Lancer les slaves qui vont demander aux serveurs la suppression des
		 * morceaux du fichier
		 */
		launchSlavesDelete(repBlocs, hdfsFname);

		System.out.println("Fichier effacé du service Hdfs.");
	}

	public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor)
			throws UnknownHostException, IOException, InterruptedException, ClassNotFoundException {
		/*
		 * Vérifier qu'il y a assez de serveurs disponibles pour répliquer le
		 * fichier
		 */
		if (!(checkRepFactor(repFactor))) {
			System.out.println(
					"Il n'y a pas assez de serveurs disponibles pour répliquer le fichier " + repFactor + " fois");
			return;
		}

		/* Vérifier si le fichier n'existe pas déjà */
		if (checkCatalog(localFSSourceFname)) {
			System.out.println("Il existe déjà un fichier avec ce nom. Veuillez commencer par supprimer ce fichier.");
			return;
		}

		// Découper localement le fichier en morceaux de taille fixe
		int nbChunks = HdfsUtil.splitFile("../data/" + localFSSourceFname, HdfsUtil.chunkSize);

		INode fileNode = new INode(localFSSourceFname, repFactor, nbChunks);

		/* Récupérer auprès du NameNode la répartition des blocs de fichier */
		HashMap<Integer, ArrayList<String>> repBlocs = HdfsUtil.getStrategieRepartition(fileNode, Commande.CMD_WRITE);

		ArrayList<SlaveHdfsClientWrite> slaveList = new ArrayList<SlaveHdfsClientWrite>();

		/*
		 * Lancer les slaves qui vont écrire les morceaux de fichier sur les
		 * serveurs
		 */
		launchSlavesWrite(repBlocs, slaveList, localFSSourceFname, fmt);

		// Supprimer les morceaux de fichier locaux
		deleteLocalFiles(repBlocs, slaveList, localFSSourceFname);

		System.out.println("ECRITURE TERMINEE");
	}

	public static void HdfsRead(String hdfsFname, String localFSDestFname)
			throws UnknownHostException, IOException, ClassNotFoundException, InterruptedException {

		INode fileNode = new INode(hdfsFname);

		/* Quitter si le fichier demandé n'existe pas */
		if (!(checkCatalog(hdfsFname))) {
			System.out.println("Ce fichier n'existe pas.");
			return;
		}

		/* Récupérer auprès du NameNode la répartition des blocs de fichier */
		HashMap<Integer, ArrayList<String>> repBlocs = HdfsUtil.getStrategieRepartition(fileNode, Commande.CMD_READ);

		ArrayList<SlaveHdfsClientRead> slaveList = new ArrayList<SlaveHdfsClientRead>();

		/*
		 * Lancer les slaves qui vont lire les morceaux de fichier auprès des
		 * serveurs
		 */
		launchSlavesRead(repBlocs, slaveList, hdfsFname, localFSDestFname);

	}

	private static void deleteLocalFiles(HashMap<Integer, ArrayList<String>> repBlocs,
			ArrayList<SlaveHdfsClientWrite> slaveList, String localFSSourceFname)
			throws InterruptedException, IOException {
		for (SlaveHdfsClientWrite sl : slaveList) {
			sl.join();
		}

		for (Integer i : repBlocs.keySet()) {
			Files.delete(Paths.get("../data/" + localFSSourceFname + i));
		}

	}

	private static void launchSlavesWrite(HashMap<Integer, ArrayList<String>> repBlocs,
			ArrayList<SlaveHdfsClientWrite> slaveList, String localFSSourceFname, Type fmt)
			throws UnknownHostException, IOException {

		// Ecrire les morceaux de fichier sur les serveurs HDFS
		SlaveHdfsClientWrite slave;
		for (Integer i : repBlocs.keySet()) {
			System.out.println("écriture du bloc numéro " + i);
			for (String serveur : repBlocs.get(i)) {
				slave = new SlaveHdfsClientWrite(serveur, 8090, localFSSourceFname, fmt, i);
				slaveList.add(slave);
				slave.start();
			}
		}

	}

	private static void launchSlavesRead(HashMap<Integer, ArrayList<String>> repBlocs,
			ArrayList<SlaveHdfsClientRead> slaveList, String hdfsFname, String localFSDestFname)
			throws UnknownHostException, IOException, ClassNotFoundException, InterruptedException {

		SlaveHdfsClientRead currentSlave = null;

		/* Lire les morceaux de fichier */
		for (Integer i : repBlocs.keySet()) {
			/*
			 * Se connecter à 1 serveur qui contient le bloc i. Si il n'est pas
			 * disponible, essayer avec le serveur suivant
			 */
			boolean chunkObtained = false;
			int k = 0;
			for (String s : repBlocs.get(i)) {
				System.out.println(s);
			}
			while (!(chunkObtained) && (k < repBlocs.get(i).size())) {
				try {
					currentSlave = new SlaveHdfsClientRead(repBlocs.get(i).get(k), 8090, hdfsFname + i);
					chunkObtained = true;
				} catch (ConnectException e) {
					k++;
				}
			}
			if (k == repBlocs.get(i).size()) {
				System.out.println("Aucun serveur disponible ne possède le chunk numéro " + i + " du fichier");
				return;
			}
			System.out.println(i);
			slaveList.add(currentSlave);
			currentSlave.start();
		}
		/* Ecrire le fichier sur le système de fichiers local */
		Format file = (Format) new LineFormat("../data/" + localFSDestFname);
		file.open(Format.OpenMode.W);

		ObjectInputStream ois;
		KV res;
		for (SlaveHdfsClientRead slave : slaveList) {
			slave.join();
			ois = slave.getObjectInputStream();
			while ((res = (KV) ois.readObject()) != null) {
				file.write(res);
			}
			ois.close();
		}
	}

	private static void launchSlavesDelete(HashMap<Integer, ArrayList<String>> repBlocs, String hdfsFname)
			throws IOException {

		SlaveHdfsClientDelete sl;
		for (Integer i : repBlocs.keySet()) {
			for (String serveur : repBlocs.get(i)) {
				try {
					sl = new SlaveHdfsClientDelete(serveur, 8090, hdfsFname + i);
					sl.start();
				} catch (ConnectException e) {
					System.out.println("Le serveur " + serveur + " est actuellement indisponible. Les chunks"
							+ " stockés sur ce serveur n'ont pas pu être supprimés.");
				}
			}
		}
	}

	/*
	 * Méthode qui vérifie si un fichier est présent dans le catalogue de
	 * fichiers du NameNode
	 */
	private static boolean checkCatalog(String filename)
			throws UnknownHostException, ClassNotFoundException, IOException {
		boolean fileFound = false;
		ArrayList<INode> listeFichiers = HdfsUtil.getListINodes();
		for (INode in : listeFichiers) {
			if (in.getFilename().equals(filename)) {
				fileFound = true;
			}
		}
		return fileFound;
	}

	private static boolean checkRepFactor(int repFactor)
			throws UnknownHostException, ClassNotFoundException, IOException {
		HashMap<String, Integer> availableServers = HdfsUtil.getAvailableServers();
		if (repFactor > availableServers.size()) {
			return false;
		} else {
			return true;
		}
	}

	public static void main(String[] args) {
		// java HdfsClient <read|write> <line|kv> <file>

		/* Lire la ligne de commande */
		parseCommandLine(args);

	}

	private static void parseCommandLine(String[] args) {
		try {
			if (args.length < 2) {
				usage();
				return;
			}

			switch (args[0]) {
			case "read":
				if (args.length == 2) {
					HdfsRead(args[1], args[1] + "READ");
				} else {
					HdfsRead(args[1], args[2]);
				}
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
				if (args[1].equals("line"))
					fmt = Format.Type.LINE;
				else if (args[1].equals("kv"))
					fmt = Format.Type.KV;
				else {
					usage();
					return;
				}
				/* Ecrire le fichier sur hdfs */
				if (args.length == 4) {
					/* Si l'utilisateur précise un facteur de réplication */
					HdfsWrite(fmt, args[2], Integer.parseInt(args[3]));
				} else {
					/* Sinon, facteur de réplication par défaut */
					HdfsWrite(fmt, args[2], HdfsUtil.defaultRepFactor);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
