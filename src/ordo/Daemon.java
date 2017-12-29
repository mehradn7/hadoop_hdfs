package ordo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;
import java.util.HashMap;

import map.Mapper;
import map.Reducer;
import formats.Format;
import formats.Format.OpenMode;
import formats.FormatWriter;
import formats.RLineWSocketFormat;
import formats.KV;

public class Daemon extends UnicastRemoteObject implements IDaemon {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static String rmiHost = "192.168.1.11"; // TODO
	public static int rmiPort = 5000;
	public static int hbPort = 5002;
	
	private String localHost = null;
	private int localPort = 0;
	private HeartBeatEmitter hb;
	private Collection<KV> results;
	
	
	public Daemon(String localHost, int port) throws UnknownHostException, IOException {
		super();
		this.localHost = localHost;
		this.localPort = port;
		this.hb = new HeartBeatEmitter(Daemon.rmiHost, Daemon.hbPort);
		this.hb.start();
		
	}
	
	public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
		/*
		 * On crée un thread esclave qui va exécuter le map
		 */
		MapSlave s = new MapSlave(reader, writer, m, this.results, cb, this.localHost, this.localPort);
		s.start();
	}
	
	public void runReduce (Reducer r, Format reader, Format writer, CallBack cb) throws RemoteException {
		/*
		 * On crée un thread esclave qui va exécuter le reduce
		 */
		ReduceSlave s = new ReduceSlave(reader, writer, r, results, cb, this.localHost, this.localPort);
		s.start();
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

class MapSlave extends Thread {

	private Format reader;
<<<<<<< HEAD
	private Format writer;
	private Collection<KV> results;
=======
	private RLineWSocketFormat writer;
	private HashMap<String, Integer> results;
>>>>>>> 4dad24d9aee74cac126c67ee1180b5ece363a782
	private CallBack cb;
	private Mapper m;
	private String localHost;
	private int localPort;


	public MapSlave(Format reader, Format writer, Mapper m, 
			Collection<KV> results, CallBack cb, String localHost, int localPort) {
		this.reader = reader;
		/* On ouvre les fichiers utiles au map */
		this.reader= reader;
		this.writer=(RLineWSocketFormat)writer;
		this.cb = cb;
		this.results = results;
		this.writer.setKvs(this.results);
		this.m = m;
	}

	public void run() {

		this.reader.open(OpenMode.R);
		this.writer.open(OpenMode.W);
		this.m.map(reader, (FormatWriter) writer);

		this.cb.isTerminated(this.localHost, this.localPort);
	}
}

class ReduceSlave extends Thread {

	private ServerSocket ss;
	private Socket s;
	private Format reader;
	private Format writer;
	private Collection<KV> results;
	private CallBack cb;
	private Reducer r;
	private String localHost;
	private int localPort;


	public ReduceSlave(Format reader, Format writer, Reducer r, 
			Collection<KV> results, CallBack cb, String localHost, int localPort) {
		this.reader = reader;
		/* On ouvre les fichiers utiles au map */
		this.reader= reader;
		this.writer=writer;
		this.cb = cb;
		this.results = results;
		this.r = r;
	}

	public void run() {
		// TODO

		this.cb.isTerminated(this.localHost, this.localPort);
	}
}

