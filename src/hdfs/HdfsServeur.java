package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.swing.plaf.synth.SynthSeparatorUI;

import formats.Format;
import formats.KV;
import formats.KvFormat;
import formats.LineFormat;

public class HdfsServeur {

	public static String prefix = "hdfs";
	public static int port;
	public static Format file;
	public static Socket s;
	public static String hstname;
	public static String prefixlog;

	/**
	 * Permet de créer un noeud (serveur) hdfs commande : java hdfsserveur port
	 * 
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		// port = Integer.parseInt(args[0]);
		HdfsServeur.port = 8090;
		HdfsServeur.hstname = InetAddress.getLocalHost().getHostName();

		/* Signaler la connexion au NameNode */

		Socket sn = new Socket(NameNode.hostname, NameNode.port);
		ObjectOutputStream oos = new ObjectOutputStream(sn.getOutputStream());
		oos.writeObject("connect");
		oos.writeObject(hstname);
		oos.writeInt(port);
		oos.close();
		sn.close();

		/* Attendre les commandes du client */
		HdfsServeur.prefixlog = "[" + HdfsServeur.hstname + ":" + HdfsServeur.port + "] : ";
		@SuppressWarnings("resource") // libérée automatiquement à la fin du
										// process
		ServerSocket ss = new ServerSocket(port);
		try {
			String[] tmp = Paths.get("").toAbsolutePath().toString().split("/");
			HdfsServeur.prefix = "/" + tmp[1] + "/" + tmp[2] + "/" + HdfsServeur.prefix;
			try {
				Files.createDirectory(Paths.get(HdfsServeur.prefix));
			} catch (FileAlreadyExistsException e) {
			} // besoin de rien besoin de tout
			HdfsServeur.prefix = HdfsServeur.prefix + "/files-" + HdfsServeur.hstname;
			Files.createDirectory(Paths.get(HdfsServeur.prefix));
		} catch (FileAlreadyExistsException e) {
		} // le fichier est deja cree besoin de rien
		System.out.println(HdfsServeur.prefixlog + "Le serveur est lancé.");

		String cmd = "";
		ObjectInputStream ois = null;
		while (true) {
			System.out.println("SERVER READY");
			s = ss.accept(); // Bloquante
			System.out.println(HdfsServeur.prefixlog + "Le serveur accepte une nouvelle connexion.");
			try {
				ois = new ObjectInputStream(s.getInputStream());
				cmd = (String) ois.readObject();
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println(HdfsServeur.prefixlog + "Action demandée : " + cmd);
			switch (cmd) {
			case "write":
				// ce genre de try sert à éviter de faire crash le serveur par
				// une mauvaise manip du client
				try {
					write(ois);
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println(HdfsServeur.prefixlog + "Erreur...");
				}
				break;
			case "delete":
				try {
					delete(ois);
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println(HdfsServeur.prefixlog + "Erreur...");
				}
				break;
			case "read":
				try {
					read(ois);
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println(HdfsServeur.prefixlog + "Erreur...");
				}
				break;
			default:
				System.out.println(HdfsServeur.prefixlog + "Erreur sur la commande...");
				break;
			}
		}
	}

	public static void write(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		KV res;
		String fileType = (String) ois.readObject();
		String fileName = (String) ois.readObject();
		Format fmt = HdfsServeur.getFormat(fileType, fileName);
		fmt.open(Format.OpenMode.W);
		System.out.println(HdfsServeur.prefixlog + "Writing : " + fileName);
		while ((res = (KV) ois.readObject()) != null) {
			fmt.write(res);
		}
		fmt.close();
		System.out.println(HdfsServeur.prefixlog + "Le serveur a fini d'écrire : " + fileName);
	}

	public static void read(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		String fileName = (String) ois.readObject();
		System.out.println(HdfsServeur.prefixlog + "Lecture de : " + fileName);
		Format fmt = (Format) new LineFormat(HdfsServeur.prefix + "/" + fileName);
		fmt.open(Format.OpenMode.R);
		KV res;
		ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
		while ((res = fmt.read()) != null) {
			oos.writeObject(res);
		}
		fmt.close();
		oos.writeObject(null);
		System.out.println(HdfsServeur.prefixlog + "Le serveur a fini de lire : " + fileName);
	}

	public static void delete(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		String fileName = (String) ois.readObject();
		try {
			Files.delete(Paths.get(HdfsServeur.prefix + "/" + fileName));
			System.out.println(HdfsServeur.prefixlog + "suppression du fichier -> " + fileName);
		} catch (java.nio.file.NoSuchFileException e) {
			System.out.println(HdfsServeur.prefixlog + "fichier introuvable : " + fileName);
		}
	}

	public static Format getFormat(String fileType, String fileName) {
		Format file;
		String filePath = HdfsServeur.prefix + "/" + fileName;
		switch (fileType) {
		case "LINE":
			file = (Format) new LineFormat(filePath);
			break;
		case "KV":
			file = (Format) new KvFormat(filePath);
			break;
		default:
			file = null;
			System.out.println(HdfsServeur.prefixlog + "Format non reconnu !!!");
		}
		return file;
	}
}