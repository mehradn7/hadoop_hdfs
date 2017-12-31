package formats;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class SocketFormat implements Format {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/*
	 * Indique la position dans le "fichier" courant.
	 */
	private long index;
	
	/*
	 * Serveur Socket de communication.
	 */
	private ServerSocket ss;
	
	/*
	 * Sockets sur lesquelles ont reçoit.
	 */
	private Collection<Socket> sockets;
	
	/*
	 * Numéro du port de l'hôte avec lequel communique le socket.
	 */
	private int port;
	
	/*
	 * Nom du fichier théoriquement reçu ('inutile').
	 */
	private String fname;
	
	private SlaveOpen slaveOpen;
	
	public SocketFormat(int port) {
		this.index = 1L;
		this.port = port;
	}

	@Override
	public KV read() {
		KV res = null;
		/*
		 * On retourne le premier KV lu sur les sockets entrants.
		 */
		for(Socket s : this.sockets) {
			try {
				res = (KV) (new ObjectInputStream(s.getInputStream())).readObject();
				if (res != null) {
					return res;
				}
			} catch (ClassNotFoundException | IOException e) {
				e.printStackTrace();
			}
		}
		return res;
	}
	
	@Override
	public void write(KV record) {
	}

	@Override
	public void open(OpenMode mode) {
		if (mode == OpenMode.R) {
			// Ouverture en mode lecture.
			try {
				this.ss = new ServerSocket(this.port);
				this.sockets = new ArrayList<Socket>();
				
				/*
				 * On lance l'acceptation de nouvelles connexions en parallèle.
				 */
				this.slaveOpen = new SlaveOpen(this.ss, this.sockets);
				this.slaveOpen.start();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void close() {
		/*
		 * On arrête d'accepter de nouvelles connexions.
		 */
		this.slaveOpen.interrupt();
		
		try {
			this.ss.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		/*
		 * On ferme toutes les sockets.
		 */
		for(Socket s : this.sockets) {
			try {
				s.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public long getIndex() {
		return this.index;
	}

	@Override
	public String getFname() {
		return this.fname;
	}

	@Override
	public void setFname(String fname) {
		this.fname = fname;
	}
}

/*
 * Classe esclave qui regarde en permanence l'arrivée de nouvelles connexions sur lesquelles lire
 * et les ajoutes à une collection.
 */
class SlaveOpen extends Thread {
	
	ServerSocket ss;
	Collection<Socket> sockets;
	
	public SlaveOpen(ServerSocket ss, Collection<Socket> sockets) {
		this.ss = ss;
		this.sockets = sockets;
	}
	
	public void run() {
		Socket tmp_s;
		while(true) {
			try {
				tmp_s = this.ss.accept();
				synchronized(this.sockets) {
					this.sockets.add(tmp_s);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}	
}

