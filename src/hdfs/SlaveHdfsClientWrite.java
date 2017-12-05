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
	private int startLine;
	private int chunkSize;
	private Format.Type fileType;
	private Format file;
	private String host;
	private int port;
	private String fname;
	
	public SlaveHdfsClientWrite(String host, int port, int startLine, int chunkSize,
			String localFSSourceFname, Format.Type fmt) throws UnknownHostException, IOException {
		this.host = host;
		this.port = port;
		this.s = new Socket(host, port);
		this.startLine = startLine;
		this.chunkSize = chunkSize;
		this.fileType = fmt;
		this.fname = localFSSourceFname;
		String pathString = "../data/"+localFSSourceFname;
		switch(fmt) {
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
		//System.out.println("Écriture commencée...");
		int i;
		file.open(OpenMode.R);
		try {
			for (i = 1; i < startLine && (file.read()) != null; i++) {};
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			oos.writeObject("write");
			oos.writeObject(fileType.toString());
			oos.writeObject(this.fname);
			KV kv;
			for (i = 1; ((kv = file.read()) != null) && (i <= this.chunkSize); i++ ) {
				oos.writeObject(kv);
			}
			oos.writeObject(null);
			s.close();
			//System.out.println("Écriture terminée !");
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
}

