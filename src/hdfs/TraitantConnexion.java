package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;

import formats.Format;
import formats.KV;
import formats.KvFormat;
import formats.LineFormat;

public class TraitantConnexion implements Runnable {

	protected Socket socket;
	protected String hstname;
	protected int port;
	protected String prefixlog = "";
	protected String cmd = "";
	protected String prefix = "";

	public TraitantConnexion(Socket s, String hostname, int port, String prefixPath) {
		this.socket = s;
		this.hstname = hostname;
		this.port = port;
		this.prefixlog = "[" + this.hstname + ":" + this.port + "] : ";
		this.prefix = prefixPath;
	}

	@Override
	public void run() {
		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(socket.getInputStream());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println(this.prefixlog + "Le serveur accepte une nouvelle connexion.");
		try {

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
		try {
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
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
		ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
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
