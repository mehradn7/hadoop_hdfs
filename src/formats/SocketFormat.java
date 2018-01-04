package formats;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

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
	
	private CountDownLatch barrierRead;
	
	public SocketFormat(int port) {
		this.index = 1L;
		this.port = port;
		this.barrierRead = new CountDownLatch(1);
	}

	@Override
	public KV read() {
		KV res = null;
		ObjectInputStream oos;
		/*
		 * On retourne le premier KV lu sur les sockets entrants.
		 */
		if (this.sockets.size() == 0) {
			try {
				this.barrierRead.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		Iterator<Socket> it_sockets = this.sockets.iterator();
		Socket s;
		while(it_sockets.hasNext()) {
			s = it_sockets.next();
			try {
				System.out.println("lecture sur socket");
				if (!s.isClosed()) {
					oos = (new ObjectInputStream(s.getInputStream()));
					res = (KV) oos.readObject();
					System.out.println("RES : "+res);
					if (res != null) {
						return res;
					} else {
						oos.close();
						s.close();
					}
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
				System.out.println("Ouverture du reader : socketFormat");
				this.ss = new ServerSocket(this.port);
				this.sockets = new ArrayList<Socket>();
				
				/*
				 * On lance l'acceptation de nouvelles connexions en parallèle.
				 */
				this.slaveOpen = new SlaveOpen(this.ss, this.sockets, this.barrierRead);
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
	CountDownLatch barrierRead;
	
	public SlaveOpen(ServerSocket ss, Collection<Socket> sockets, CountDownLatch barrierRead) {
		this.ss = ss;
		this.sockets = sockets;
		this.barrierRead = barrierRead;
	}
	
	public void run() {
		Socket tmp_s;
		while(true) {
			try {
				tmp_s = this.ss.accept();
				System.out.println("Nouvelle connexion");
				synchronized(this.sockets) {
					this.sockets.add(tmp_s);
					this.barrierRead.countDown();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}	
}

