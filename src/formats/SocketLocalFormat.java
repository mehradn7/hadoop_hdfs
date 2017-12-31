package formats;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;

public class SocketLocalFormat implements Format {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/*
	 * Indique la position dans le "fichier" courant.
	 */
	private long index;
	
	/*
	 * Socket lié à this.hostname:this.port.
	 */
	private Socket s;
	
	/*
	 * Nom de l'hôte avec lequel communique le socket.
	 */
	private String hostname;
	
	/*
	 * Numéro du port de l'hôte avec lequel communique le socket.
	 */
	private int port;
	
	/*
	 * ObjectOutputStream lié à la socket this.s.
	 */
	private ObjectOutputStream oos;
	
	/*
	 * ObjectInputStream lié à la socket this.s.
	 */
	private ObjectInputStream ois;
	
	/*
	 * Stockage local des KVs résultats.
	 */
	private HashMap<String, Integer> results; // TODO : le type devrait être paramétrable
	
	public SocketLocalFormat(String hostname, int port) {
		this.index = 1L;
		this.hostname = hostname;
		this.port = port;
		this.results = new HashMap<String, Integer>();
	}

	@Override
	public KV read() {
		// TODO : utile ?
		return null;
	}
	
	/*
	 * (non-Javadoc)
	 * @see formats.FormatWriter#write(formats.KV)
	 * on envoie une clef via la socket lorsqu'elle est nouvelle,
	 * on ajoute le KV à la HashMap Correspondante this.results.
	 */
	@Override
	public void write(KV record) {
		try{
			if (!this.results.containsKey(record.k)) {
				this.oos.writeObject(record.k);
				this.results.put(record.k, Integer.valueOf(record.v));
			} else {
				this.results.put(record.k, Integer.valueOf(record.v)+this.results.get(record.k));
			}
		}catch(IOException e) {
			e.printStackTrace();
		}
		this.index++;
	}

	@Override
	public void open(OpenMode mode) {
		this.index = 1L;
		if (mode == OpenMode.R) {
			try {
				this.s = new Socket(this.hostname, this.port);
				this.oos = new ObjectOutputStream(this.s.getOutputStream());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}else {
			// TODO : utile ?
		}
	}

	@Override
	public void close() {
		try {
			this.s.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public long getIndex() {
		return this.index;
	}

	@Override
	public String getFname() {
		return this.hostname;
	}

	@Override
	public void setFname(String host) {
		this.hostname = host;
	}

	public Socket getS() {
		return s;
	}

	public void setS(Socket s) {
		this.s = s;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getHost() {
		return hostname;
	}

	public void setHost(String host) {
		this.hostname = host;
	}

	public ObjectOutputStream getOos() {
		return oos;
	}

	public void setOos(ObjectOutputStream oos) {
		this.oos = oos;
	}

	public ObjectInputStream getOis() {
		return ois;
	}

	public void setOis(ObjectInputStream ois) {
		this.ois = ois;
	}

	public HashMap<String, Integer> getResults() {
		return this.results;
	}

	public void setResults(HashMap<String, Integer> results) {
		this.results = results;
	}

	public void setIndex(long index) {
		this.index = index;
	}
}
