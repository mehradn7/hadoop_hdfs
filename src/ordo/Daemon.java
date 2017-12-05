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

	public static String rmHost = "localhost"; // TODO
	public static int rmPortRMI = 5000;
	public static int rmPortHB = 5001;
	
	public String localHost = null;
	public int localPort = 0;
	public HeartBeatEmitter hb;
	
	public Daemon(String localHost, int port) throws UnknownHostException, IOException {
		super();
		this.hb = new HeartBeatEmitter(Daemon.rmHost, Daemon.rmPortHB);
		this.hb.start();
		
	}
	
	public void runMap (Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
	}
	
	@Override
	public String getHostname() {
		return this.localHost;
	}
	
	public static void main(String args[]) {
		try {
			int localPort = Integer.parseInt(args[0]);
			ILauncher l = (ILauncher) Naming.lookup("//"+Daemon.rmHost+":"+Daemon.rmPortRMI+"Launcher");
			l.addDaemon(new  Daemon(InetAddress.getLocalHost().getHostName(), localPort));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
