package hdfs;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;

import formats.Format;
import ordo.HeartBeatEmitter;

public class HdfsServeur implements Runnable {

	protected String prefix = "nosave/hdfs";
	protected int port = 8090;
	protected Format file;
	protected Socket s;
	protected ServerSocket ss;
	protected String hstname;
	protected String prefixlog;
	private HeartBeatEmitter heartBeatThread;

	public void run() {
		try {
			//this.hstname = InetAddress.getLocalHost().getHostName();
			this.hstname = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}

		/* Signaler la première connexion au NameNode */
		connectToNameNode();

		/* Lancer un thread chargé d'envoyer des HeartBeats au NameNode */
		runHeartBeatEmitter();

		/* Ouvrir le ServerSocket */
		openServerSocket(port);

		/* Créer les dossiers contenant les fichiers stockés sur ce serveur */
		createFolders();

		this.prefixlog = "[" + this.hstname + ":" + this.port + "] : ";

		System.out.println(this.prefixlog + "Le serveur est lancé.");

		while (true) {
			try {
				s = ss.accept();
			} catch (IOException e1) {
				e1.printStackTrace();
			}

			new Thread(new TraitantConnexion(s, this.hstname, this.port, this.prefix)).start();

		}
	}

	private void createFolders() {
		try {
			String[] tmp = Paths.get("").toAbsolutePath().toString().split("/");
			this.prefix = "/" + tmp[1] + "/" + tmp[2] + "/" + this.prefix;

			try {
				Files.createDirectory(Paths.get(this.prefix));
			} catch (FileAlreadyExistsException e) {
				// ne rien faire
			}
			this.prefix = this.prefix + "/files-" + this.hstname;
			Files.createDirectory(Paths.get(this.prefix));

		} catch (FileAlreadyExistsException e1) {
			// ne rien faire
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void openServerSocket(int port) {
		try {
			ss = new ServerSocket(port);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void runHeartBeatEmitter() {
		try {
			this.heartBeatThread = new HeartBeatEmitter(NameNode.hostname, NameNode.heartBeatPort);
			this.heartBeatThread.start();
		} catch (IOException e2) {
			e2.printStackTrace();
		}
	}

	private void connectToNameNode() {
		try {
			Socket sn = new Socket(NameNode.hostname, NameNode.port);
			ObjectOutputStream oos = new ObjectOutputStream(sn.getOutputStream());
			oos.writeObject("connect");
			oos.writeObject(hstname);
			oos.writeInt(port);
			oos.close();
			sn.close();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
}