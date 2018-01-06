package hdfs;

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

import formats.Format;
import formats.KV;
import formats.KvFormat;
import formats.LineFormat;

public class HdfsServeur implements Runnable {

	protected String prefix = "hdfs";
	protected int port;
	protected Format file;
	protected Socket s;
	protected String hstname;
	protected String prefixlog;

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

		/* Signaler la connexion au NameNode */

		try {
			Socket sn = new Socket(NameNode.hostname, NameNode.port);
			ObjectOutputStream oos = new ObjectOutputStream(sn.getOutputStream());
			oos.writeObject("connect");
			oos.writeObject(hstname);
			oos.writeInt(port);
			oos.close();
			sn.close();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		/* Attendre les commandes du client */
		this.prefixlog = "[" + this.hstname + ":" + this.port + "] : ";
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
		System.out.println(this.prefixlog + "Le serveur est lancé.");

		String cmd = "";
		ObjectInputStream ois = null;
		while (true) {
			System.out.println("SERVER READY");
			try {
				s = ss.accept();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} // Bloquante
			System.out.println(this.prefixlog + "Le serveur accepte une nouvelle connexion.");
			try {
				ois = new ObjectInputStream(s.getInputStream());
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
		ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
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