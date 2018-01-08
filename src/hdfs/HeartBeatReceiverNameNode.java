package hdfs;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


import ordo.IHeartBeatReceiver;

public class HeartBeatReceiverNameNode extends Thread implements IHeartBeatReceiver {
	
	private ServerSocket ss;
	private Map<String, Integer> availableServers;
	private HashMap<String, Socket> sockets;
	private Iterator<Socket> it_sockets;
	
	public HeartBeatReceiverNameNode(Map<String, Integer> availableServers) throws IOException {
		super();
		this.ss = new ServerSocket(NameNode.heartBeatPort);
		this.ss.setSoTimeout(300);
		this.sockets = new HashMap<String, Socket>();
		this.availableServers = availableServers;
	}

	@Override
	public void run() {
		/*
		 * Itérateur car modification concurrente de la liste en parallèle du parcours.
		 */
		while(true) {
			try {
				this.addEmitter(this.ss.accept());
			} catch (SocketTimeoutException e2) {//timeout
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			this.it_sockets = this.sockets.values().iterator();
			Socket s;
			while(it_sockets.hasNext()) {
				s = it_sockets.next();
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
		this.sockets.put(s.getInetAddress().getHostName().replaceFirst(".enseeiht.fr", ""), s);
		
	}

	@Override
	synchronized public void removeEmitter(Socket s) {
		String hostname = s.getInetAddress().getHostName().replaceFirst(".enseeiht.fr", "");
		System.out.println("Connexion perdue avec : " + hostname);
		this.availableServers.remove(hostname);
		this.it_sockets.remove();
		try {
			s.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}

}
