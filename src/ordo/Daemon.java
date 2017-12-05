package ordo;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import map.Mapper;
import formats.Format;

public class Daemon extends UnicastRemoteObject implements IDaemon {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static String rmiHost = "192.168.1.11"; // TODO
	public static int rmiPort = 5000;
	public static int hbPort = 5002;
	
	public String localHost = null;
	public int localPort = 0;
	public HeartBeatEmitter hb;
	
	public Daemon(String localHost, int port) throws UnknownHostException, IOException {
		super();
		this.localHost = localHost;
		this.localPort = port;
		this.hb = new HeartBeatEmitter(Daemon.rmiHost, Daemon.hbPort);
		this.hb.start();
		
	}
	
	public void runMap (Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
	}
	
	public String getHostname() throws RemoteException {
		return this.localHost;
	}
	
	public static void main(String args[]) {
		try {
			int localPort = Integer.parseInt(args[0]);
			String hostname = InetAddress.getLocalHost().getHostAddress();
			ILauncher l = (ILauncher) Naming.lookup("//"+Daemon.rmiHost+":"+Daemon.rmiPort+"/Launcher");
			l.addDaemon(new  Daemon(hostname, localPort));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
