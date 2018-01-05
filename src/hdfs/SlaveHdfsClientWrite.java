package hdfs;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;

import formats.Format;
import formats.Format.OpenMode;
import formats.KV;
import formats.KvFormat;
import formats.LineFormat;

public class SlaveHdfsClientWrite extends Thread {

	private Socket s;
	private Format.Type fileType;
	private Format file;
	private String host;
	private int port;
	private String fname;
	private int idBloc;

	public SlaveHdfsClientWrite(String host, int port, String localFSSourceFname,
			Format.Type fmt, int idBloc) throws UnknownHostException, IOException {
		this.host = host;
		this.port = port;
		this.s = new Socket(host, port);
		this.fileType = fmt;
		this.fname = localFSSourceFname;
		this.idBloc = idBloc;
		String pathString = "../data/" + localFSSourceFname + this.idBloc;
		switch (fmt) {
		case LINE:
			this.file = (Format) new LineFormat(pathString);
			break;
		case KV:
			this.file = (Format) new KvFormat(pathString);
			break;
		default:
			System.out.println("Format inconnu !!!");
		}
	}

	public void run() {
		// System.out.println("Écriture commencée...");

		// 1) Découper le fichier localement en morceaux de taille fixe (faire
		// ça dans HdfsClient write)

		// 2)Ecrire PARALLELEMENT chaque morceau sur un serveur (selon la
		// stratégie de
		// répartition qui sera en attribut de la classe, i.e. une HashMap
		file.open(OpenMode.R);
		try {
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			oos.writeObject("write");
			oos.writeObject(fileType.toString());
			oos.writeObject(this.fname + this.idBloc);
			KV kv;
			while ((kv = file.read()) != null) {
				oos.writeObject(kv);
			}
			oos.writeObject(null);
			s.close();
			// System.out.println("Écriture terminée !");
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// 3) Supprimer les morceaux de fichier (faire ça dans HdfsClient write)
	}
}
