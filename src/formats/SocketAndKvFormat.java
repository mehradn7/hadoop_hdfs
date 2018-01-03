package formats;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import ordo.Job;

public class SocketAndKvFormat implements Format {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private BufferedReader br;
	private BufferedWriter bw;
	private Long index = 0L;
	private String Fname;
	private Set<String> keys;
	
	public SocketAndKvFormat(String fname){
		this.Fname = fname;
	}

	@Override
	public KV read() {
		String res;
		String[] parsing;
		KV kv = null;
		try {
			if ((res = br.readLine()) != null) {
				parsing = res.split(KV.SEPARATOR);
				kv = new KV(parsing[0], parsing[1]);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return kv;
	}

	@Override
	public void write(KV record) {
		/*
		 * On ajoute la clef à l'ensemble des clefs connues.
		 */
		this.keys.add(record.k);
		/*
		 * On écrit dans un fichier les KV sous forme "k<->v"  
		 */
		try{
			bw.write(record.k+KV.SEPARATOR+record.v);
			bw.newLine();
			bw.flush();
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	@Override
	public void open(OpenMode mode) {
		if (mode == OpenMode.R) {
			try {
				br = Files.newBufferedReader(Paths.get(getFname()), Charset.forName("UTF-8"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			this.keys = new HashSet<String>();
			try {
				bw = Files.newBufferedWriter(Paths.get(getFname()), Charset.forName("UTF-8"));
			} catch (IOException e) {
				e.printStackTrace();
			}		
		}
	}

	@Override
	public void close() {
		if (br != null){
			/*
			 * Fermeture du buffer de lecture.
			 */
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}else{
			/*
			 * Lors de la fermeture en mode écriture on envoie même temps nos KVs au Job.
			 */
			try {
				Socket s = new Socket(Job.inetAddress,Job.portMapperKeys);
				ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
				oos.writeObject(this.keys);
				s.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			/*
			 * Fermeture du buffer d'écriture.
			 */
			try {
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}
	}

	@Override
	public long getIndex() {
		return (this.index++);
	}

	@Override
	public String getFname() {
		return this.Fname;
	}

	@Override
	public void setFname(String fname) {
		this.Fname = fname;
	}

}
