package ordo;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Iterator;

public class HeartBeatReceiver extends Thread implements IHeartBeatReceiver {
	
	/*
	 * Port d'écoute du HeartBeatReceiver.
	 */
	public static int port = 5002;
	
	private ServerSocket ss;
	private HashMap<String, IDaemon> daemons;
	private HashMap<String, Socket> sockets;
	
	public HeartBeatReceiver(HashMap<String, IDaemon> daemons) throws IOException {
		super();
		this.ss = new ServerSocket(HeartBeatReceiver.port);
		this.ss.setSoTimeout(300);
		this.sockets = new HashMap<String, Socket>();
		this.daemons = daemons;
	}

	@Override
	public void run() {
		/*
		 * Itérateur car modification concurrente de la liste en parallèle du parcours.
		 */
		Iterator<Socket> it_sockets;
		while(true) {
			try {
				this.addEmitter(this.ss.accept());
			} catch (SocketTimeoutException e2) {
				e2.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			it_sockets = this.sockets.values().iterator();
			for(Socket s = it_sockets.next(); it_sockets.hasNext(); s = it_sockets.next()) {
				try {
					if (s.getInputStream().read() != 1) {
						this.removeEmitter(s);
					}
				} catch (IOException e) {
					this.removeEmitter(s);
				}
			}
		}
	}

	@Override
	synchronized public void addEmitter(Socket s) {
		this.sockets.put(s.getLocalSocketAddress().toString().split("(/|:)")[1], s);
	}

	@Override
	synchronized public void removeEmitter(Socket s) {
		String ip = s.getLocalSocketAddress().toString().split("(/|:)")[1];
		this.daemons.remove(ip);
		this.sockets.remove(ip);
		try {
			s.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}

}
