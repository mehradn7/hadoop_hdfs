package ordo;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.HashMap;

public class HeartBeatReceiver extends Thread implements IHeartBeatReceiver {

	private int localPort;
	private ServerSocket ss;
	private HashMap<String, IDaemon> daemons;
	private HashMap<String, Socket> sockets;
	private Object lock = new Object();
	
	public HeartBeatReceiver(HashMap<String, IDaemon> daemons) throws IOException {
		super();
		this.localPort = 5002;
		this.ss = new ServerSocket(this.localPort);
		this.ss.setSoTimeout(300);
		this.sockets = new HashMap<String, Socket>();
		this.daemons = daemons;
	}

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
						System.out.println("Remove 1");
					}
				} catch (IOException e) {
					this.removeEmitter(s);
					System.out.println("Remove 2");
				}
			}
		}
	}

	@Override
	synchronized public void addEmitter(Socket socket) {
		this.sockets.put(socket.getInetAddress().getHostName(), socket);
		System.out.println("ADD : "+socket.getLocalAddress());
	}

	@Override
	synchronized public void removeEmitter(Socket s) {
		this.daemons.remove(s.getInetAddress().getHostName());
		System.out.println("-> "+s.getInetAddress().getHostName());
		System.out.println("-> "+this.daemons.keySet());
		this.sockets.remove(s.getInetAddress().getHostName());
		try {
			s.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}

}
