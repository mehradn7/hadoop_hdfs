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

import javax.swing.plaf.synth.SynthSeparatorUI;

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
		this.prefixlog = "[" + this.hstname + ":" + this.port + "] : ";

		System.out.println(this.prefixlog + "Le serveur est lancé.");

		while (true) {
			try {
				System.out.println("LAAA");
				s = ss.accept();
				System.out.println("SERVER READY");
			} catch (IOException e1) {
				e1.printStackTrace();
			} // Bloquante

			new Thread(new TraitantConnexion(s, this.hstname, this.port, this.prefix)).start();

		}
	}
}