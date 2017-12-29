package ordo;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;

import map.Mapper;
import map.Reducer;
import formats.Format;
import formats.Format.OpenMode;
import formats.FormatWriter;
import formats.KV;

/**
 * Daemon du service Hidoop. Cette classe est chargée d'effectuer les tâches Map et Reduce.
 * @author ÉGELÉ Romain, DIOCHOT David, PRIOU Cyrille
 *
 */
public class Daemon extends UnicastRemoteObject implements IDaemon {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/*
	 * Nom de l'hôte du registry (RMI), normalement lancé lors de l'instanciation du Job.
	 */
	public static String rmiHost = "192.168.1.11";
	
	/*
	 * Port de l'hôte du registry (RMI) normalement lancé lors de l'instanciation du Job.
	 */
	public static int rmiPort = 5000;
	
	/*
	 * TODO
	 */
	private String localHostname = null;
	
	/*
	 * TODO
	 */
	private int localPort = 0;
	
	/*
	 * Poignée du HeatBeatEmitter hb.
	 */
	private HeartBeatEmitter hb;
	
	/*
	 * Résultats de la tâche Map.
	 */
	private Collection<KV> mapResults;
	
	/*
	 * Résultats de la tâche Reduce.
	 */
	private Collection<KV> reduceResults;
	
	public Daemon(String localHostname, int port) throws UnknownHostException, IOException {
		super();
		this.localHostname = localHostname;
		this.localPort = port;
		this.hb = new HeartBeatEmitter(Daemon.rmiHost, HeartBeatReceiver.port);
		this.hb.start();
		
	}
	
	public void runMap(Mapper mapper, Format reader, Format writer, ICallBack cb) throws RemoteException {
		/*
		 * On crée un thread esclave qui va exécuter le map.
		 */
		MapSlave mapperSlave = new MapSlave(reader, writer, mapper, this.mapResults, cb, this.getLocalHostname(),
				this.localPort);
		mapperSlave.start();
	}
	
	public void runReduce (Reducer reducer, Format reader, Format writer, 
			ICallBack cb) throws RemoteException {
		/*
		 * On crée un thread esclave qui va exécuter le reduce
		 */
		ReduceSlave reducerSlave = new ReduceSlave(reader, writer, reducer, this.reduceResults, cb, this.getLocalHostname(), 
				this.localPort);
		reducerSlave.start();
	}
	
	public void setLocalHostname(String hostname) {
		this.localHostname = hostname;
	}
	
	public String getLocalHostname() throws RemoteException {
		return this.localHostname;
	}
	
	public static void main(String args[]) {
		try {
			int localPort = Integer.parseInt(args[0]);
			String hostname = InetAddress.getLocalHost().getHostAddress();
			/*
			 * Récupération du launcher, ici c'est job qui joue ce rôle.
			 */
			ILauncher launcher = (ILauncher) Naming.lookup("//"+Daemon.rmiHost+":"+Daemon.rmiPort+"/Launcher");
			launcher.addDaemon(new  Daemon(hostname, localPort));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

/**
 * Processus esclave pour lancer une tâche Map, cela sert à parallèliser le lancement de plusieurs
 * tâches Map/Reduce sur le même daemon.
 */
class MapSlave extends Thread {
	
	/*
	 * Format de lecture de l'entrée du Mapper.
	 */
	private Format reader;
	
	/*
	 * Format d'écriture de la sortie du Mapper.
	 */
	private Format writer;
	
	/*
	 * TODO
	 */
	private Collection<KV> results;
	
	/*
	 * CallBack de fin du Mapper.
	 */
	private ICallBack cb;
	
	/*
	 * Mapper.
	 */
	private Mapper mapper;
	
	/*
	 * TODO
	 */
	private String localHost;
	
	/*
	 * TODO
	 */
	private int localPort;


	public MapSlave(Format reader, Format writer, Mapper mapper, Collection<KV> results, ICallBack cb, 
			String localHost, int localPort) {
		/* On ouvre les fichiers utiles au map */
		this.reader = reader;
		this.writer = writer;
		this.cb = cb;
		this.results = results;
		//this.writer.setKvs(this.results);
		this.mapper = mapper;
	}
	
	public void run() {

		this.reader.open(OpenMode.R);
		this.writer.open(OpenMode.W);
		this.mapper.map(reader, (FormatWriter) writer);

		//this.cb.isTerminated(this.localHost, this.localPort);
		this.cb.isTerminated();
	}
}

/**
 * Processus esclave pour lancer une tâche Reduce, cela sert à parallèliser le lancement de plusieurs
 * tâches Map/Reduce sur le même daemon.
 */
class ReduceSlave extends Thread {

	private Format reader;
	private Format writer;
	private ICallBack callback;
	private Reducer reducer;
	private String localHostname;
	private int localPort;


	public ReduceSlave(Format reader, Format writer, Reducer reducer, Collection<KV> results,
			ICallBack callback, String localHostname, int localPort) {
		this.reader = reader;
		this.writer = writer;
		this.reducer = reducer;
		this.callback = callback;
		this.localHostname = localHostname; 
		this.localPort = localPort;
	}

	public void run() {
		
		/*
		 * Lancement du reduce,
		 * le reader : est chargé du lancement d'un serveur d'écoute pour réception des KVs provenant des
		 * tâches Maps.
		 * le writer : est chargé d'envoyer son résultat au Job.
		 */
		this.reader.open(OpenMode.R);
		this.writer.open(OpenMode.W);
		this.reducer.reduce(this.reader, this.writer);

		/*
		 * Signalement au Job que la tâche reduce est terminée.
		 */
		this.callback.isTerminated();
	}
}

