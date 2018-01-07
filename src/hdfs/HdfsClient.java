package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
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
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <line|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}

	public static void HdfsDelete(String hdfsFname) throws UnknownHostException, IOException, ClassNotFoundException {
		INode fileNode = new INode(hdfsFname);

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
		// Découper localement le fichier en morceaux de taille fixe
		int chunkSize = 10000;// en Ko
		int nbChunks = HdfsUtil.splitFile("../data/" + localFSSourceFname, chunkSize);

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
		
		HdfsUtil.printHashMap(repBlocs);
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
		
		HdfsUtil.printHashMap(repBlocs);
		
		/* Lire les morceaux de fichier */
		SlaveHdfsClientRead currentSlave;
		for (Integer i : repBlocs.keySet()) {
			currentSlave = new SlaveHdfsClientRead(repBlocs.get(i).get(0), 8090, hdfsFname + i);
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
			throws UnknownHostException, IOException {
		
		HdfsUtil.printHashMap(repBlocs);

		SlaveHdfsClientDelete sl;
		for (Integer i : repBlocs.keySet()) {
			for (String serveur : repBlocs.get(i)) {
				sl = new SlaveHdfsClientDelete(serveur, 8090, hdfsFname + i);
				sl.start();
			}
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
				HdfsWrite(fmt, args[2], 2);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
