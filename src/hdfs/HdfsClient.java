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
import java.util.HashMap;

import formats.Format;
import formats.KV;
import formats.LineFormat;

public class HdfsClient {

	private static Path configFile = Paths
			.get("../src/config/config-serveurs.txt"); /*
														 * EN RELATIF PAR
														 * RAPPORT A BIN
														 */
	private static ArrayList<String> hl = new ArrayList<String>(); // Adresses
																	// des
																	// serveurs
	private static ArrayList<Integer> pl = new ArrayList<Integer>(); // Ports
																		// des
																		// serveurs

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

		HashMap<Integer, ArrayList<String>> repBlocs = HdfsUtil.getStrategieRepartition(fileNode, Commande.CMD_DELETE);

		for (Integer i : repBlocs.keySet()) {
			for (String server : repBlocs.get(i)) {
				System.out.println(i + "->" + server);

			}
		}

		SlaveHdfsClientDelete sl;
		for (Integer i : repBlocs.keySet()) {
			for (String serveur : repBlocs.get(i)) {
				sl = new SlaveHdfsClientDelete(serveur, 8090, hdfsFname + i);
				sl.start();
			}
		}
		System.out.println("Fichier effacé du service Hdfs.");
	}

	public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor)
			throws UnknownHostException, IOException, InterruptedException, ClassNotFoundException {
		// 1) Découper localement le fichier en morceaux de taille fixe
		int chunkSize = 10000;// en Ko
		int nbChunks = HdfsUtil.splitFile("../data/" + localFSSourceFname, chunkSize);
		System.out.println(nbChunks);

		INode fileNode = new INode(localFSSourceFname, repFactor, nbChunks);

		HashMap<Integer, ArrayList<String>> repBlocs = HdfsUtil.getStrategieRepartition(fileNode, Commande.CMD_WRITE);

		for (Integer i : repBlocs.keySet()) {
			for (String server : repBlocs.get(i)) {
				System.out.println(i + "->" + server);

			}
		}

		ArrayList<SlaveHdfsClientWrite> slaveList = new ArrayList<SlaveHdfsClientWrite>();

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

		// Supprimer les morceaux de fichier locaux
		for (SlaveHdfsClientWrite sl : slaveList) {
			sl.join();
		}

		for (Integer i : repBlocs.keySet()) {
			Files.delete(Paths.get("../data/" + localFSSourceFname + i));
		}

		System.out.println("ECRITURE TERMINEE");
	}

	public static void HdfsRead(String hdfsFname, String localFSDestFname)
			throws UnknownHostException, IOException, ClassNotFoundException, InterruptedException {
		System.out.println("Lecture en cours ...");
		ArrayList<SlaveHdfsClientRead> sl = new ArrayList<SlaveHdfsClientRead>();

		INode fileNode = new INode(hdfsFname);

		HashMap<Integer, ArrayList<String>> repBlocs = HdfsUtil.getStrategieRepartition(fileNode, Commande.CMD_READ);

		for (Integer i : repBlocs.keySet()) {
			for (String server : repBlocs.get(i)) {
				System.out.println(i + "->" + server);

			}
		}
		SlaveHdfsClientRead currentSlave;
		for (Integer i : repBlocs.keySet()) {
			currentSlave = new SlaveHdfsClientRead(repBlocs.get(i).get(0), 8090, hdfsFname + i);
			sl.add(currentSlave);
			currentSlave.start();
		}
		Format file = (Format) new LineFormat("../data/" + localFSDestFname);
		file.open(Format.OpenMode.W);
		ObjectInputStream ois;
		KV res;
		
		for (SlaveHdfsClientRead slave : sl) {
			slave.join();
			ois = slave.getObjectInputStream();
			while ((res = (KV) ois.readObject()) != null) {
				file.write(res);
			}
			ois.close();
		}
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
			if (args.length < 2) {
				usage();
				return;
			}

			switch (args[0]) {
			case "read":
				HdfsRead(args[1], args[1] + "READ");
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
				/*
				 * Contacter le NameNode et récupérer la liste des serveurs
				 * disponibles
				 */

				/* Ecrire le fichier sur hdfs */
				HdfsWrite(fmt, args[2], 2);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
