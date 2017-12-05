package ordo;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.HashMap;

public class HeartBeatReceiver extends Thread implements IHeartBeatReceiver {

	private int localPort;
	private ServerSocket ss;
	private Collection<IDaemon> daemons;
	private HashMap<String,Boolean> emitters;
	private HashMap<String, Socket> sockets;
	private Object lock = new Object();
	
	public HeartBeatReceiver(Collection<IDaemon> daemons) throws IOException {
		super();
		this.localPort = 5002;
		this.ss = new ServerSocket(this.localPort);
		this.ss.setSoTimeout(3000);
		this.emitters = new HashMap<String,Boolean>();
		this.sockets = new HashMap<String, Socket>();
		this.daemons = daemons;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		while(true) {
			try {
				this.addEmitter(this.ss.accept());
			} catch (SocketTimeoutException e2) {
			} catch (IOException e) {
				e.printStackTrace();
			}
			for(Socket s : this.sockets.values()){
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
	synchronized public void updateLiving() {
		for(IDaemon d : this.daemons) {
			if (this.sockets.get(d.getHostname()) == null) {
				synchronized (this.lock) {
					this.daemons.remove(d);
				}
			}
		}
	}

	@Override
	public void addEmitter(Socket socket) {
		this.sockets.put(socket.getInetAddress().getHostName(), socket);
	}

	@Override
	public void removeEmitter(Socket s) {
		this.emitters.remove(s.getInetAddress().getHostName());
	}

}
