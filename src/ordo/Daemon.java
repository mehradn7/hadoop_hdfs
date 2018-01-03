package ordo;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;

import formats.Format;
import formats.Format.OpenMode;
import hdfs.HdfsServeur;
import map.Mapper;
import map.Reducer;

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
	 * Port de reception des clefs reçues pour la tâche reduce. 
	 */
	public static final int portReducersKeys = 5002;
	
	/*
	 * Nom du répertoire des données.
	 */
	private static String prefix = "hdfs";
	
	/*
	 * IP de l'hôte local sous forme de String.
	 */
	private String localHostname;
	
	/*
	 * Poignée du HeatBeatEmitter hb.
	 */
	private HeartBeatEmitter hb;
	
	/*
	 * Hashmap qui fait correspondre à une clef l'ip d'une machine.
	 */
	private HashMap<String, String> keyToDaemon;
	
	public Daemon(String localHostname) throws UnknownHostException, IOException {
		super();
		this.localHostname = localHostname;
		this.hb = new HeartBeatEmitter(Job.inetAddress, HeartBeatReceiver.port);
		this.hb.start();
	}
	
	public void runMap(Mapper mapper, Format reader, Format writer, ICallBack callbackMapper) 
			throws RemoteException {
		reader.setFname(Daemon.prefix + reader.getFname());
		writer.setFname(Daemon.prefix + writer.getFname());
		
		/*
		 * On crée un thread esclave qui va exécuter le map.
		 */
		MapSlave mapperSlave = new MapSlave(reader, writer, mapper, callbackMapper);
		mapperSlave.start();
	}
	
	public void runReduce (Reducer reducer, Format reader, Format writer, ICallBack callbackReducer)
			throws RemoteException {
		/*
		 * On crée un thread esclave qui va exécuter le reduce
		 */
		ReduceSlave reducerSlave = new ReduceSlave(reader, writer, reducer, callbackReducer);
		reducerSlave.start();
	}

	@Override
	public String getLocalHostname() throws RemoteException {
		return this.localHostname;
	}

	@Override
	public void setLocalHostname(String hostname) throws RemoteException {
		this.localHostname = hostname;
		
	}
	
	public HashMap<String, String> getKeyToDaemon() throws RemoteException {
		return keyToDaemon;
	}

	public void setKeyToDaemon(HashMap<String, String> keyToDaemon) throws RemoteException{
		this.keyToDaemon = keyToDaemon;
	}

	public static void main(String args[]) {
		try {
			String localHostname = InetAddress.getLocalHost().getHostAddress();
			
			/*
			 * Création des répertoirs si besoin. 
			 */
			try {
				String[] tmp = Paths.get("").toAbsolutePath().toString().split("/");
				Daemon.prefix = "/" + tmp[1] + "/" + tmp[2] + "/" + Daemon.prefix;
				try {
					Files.createDirectory(Paths.get(Daemon.prefix));
				} catch (FileAlreadyExistsException e) {
				} // besoin de rien besoin de tout
				Daemon.prefix = Daemon.prefix + "/files-" + localHostname + "/";
				Files.createDirectory(Paths.get(Daemon.prefix));
			} catch (FileAlreadyExistsException e) {
			} // le fichier est deja cree besoin de rien
			
			/*
			 * Récupération du launcher, ici c'est job qui joue ce rôle.
			 */
			ILauncher launcher = (ILauncher) Naming.lookup("//"+Job.inetAddress+":"+Job.portRegistryRMI+"/Launcher");
			launcher.addDaemon(new  Daemon(localHostname));
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
	 * Mapper.
	 */
	private Mapper mapper;
	
	/*
	 * CallBack de fin du Mapper.
	 */
	private ICallBack callbackMapper;


	public MapSlave(Format reader, Format writer, Mapper mapper, ICallBack callbackMapper) {
		this.reader = reader;
		this.writer = writer;
		this.mapper = mapper;
		this.callbackMapper = callbackMapper;
	}
	
	public void run() {
		
		/*
		 * Ouverture des entrées/sorties.
		 */
		this.reader.open(OpenMode.R);
		this.writer.open(OpenMode.W);
		
		/*
		 * Fonction map appliquée au fraguement local.
		 */
		this.mapper.map(reader, writer);
		
		/*
		 * Fermeture des entrées/sorties.
		 */
		this.reader.close();
		this.writer.close(); //envoie des clefs au Job.

		try {
			this.callbackMapper.isTerminated(); //indique au Job sa terminaison.
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		System.out.println("Mapper terminé.");
	}
}

/**
 * Processus esclave pour lancer une tâche Reduce, cela sert à parallèliser le lancement de plusieurs
 * tâches Map/Reduce sur le même daemon.
 */
class ReduceSlave extends Thread {

	private Format reader;
	private Format writer;
	private Reducer reducer;
	private ICallBack callback;


	public ReduceSlave(Format reader, Format writer, Reducer reducer, ICallBack callback) {
		this.reader = reader;
		this.writer = writer;
		this.reducer = reducer;
		this.callback = callback;
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
		try {
			this.callback.isTerminated();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

