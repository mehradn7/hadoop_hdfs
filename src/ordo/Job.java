package ordo;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import formats.Format.Type;
import map.MapReduce;

public class Job extends UnicastRemoteObject implements ILauncher, IJob {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/*
	 * Port du registry RMI.
	 */
	private static int portRMI = 5000; // TODO
	
	/*
	 * HashMap qui lie l'adresse d'un daemon avec la poignée du stub (RMI) de ce daemon, accès
	 * concurrent sur cette HashMap.
	 */
	private HashMap<String, IDaemon> daemons = new HashMap<String, IDaemon>();
	
	/*
	 * Poignée du HeartBeatReceiver qui surveille l'état des daemons de la HashMap "daemons"
	 * et qui met à jours cette HashMap en fonction de celui-ci.
	 */
	private HeartBeatReceiver hb;
	
	/*
	 * Nombre de tâches de type Reduce.
	 */
	private int numberOfReduces;
	
	/*
	 * Nombre de tâches de type Map.
	 */
	private int numberOfMaps;
	
	/*
	 * Format du fichier en entrée du Job.
	 */
	private Type inputFormat;
	
	/*
	 * Format du fichier en sortie du Job.
	 */
	private Type outputFormat;

	/*
	 * Nom du fichier en entrée du Job.
	 */
	private String inputFname;
	
	/*
	 * Nom du fichier en sortie du Job.
	 */
	private String outputFname;

	/*
	 * Tâche mapReduce à executer.
	 */
	private MapReduce mapReduce;
	
	/*
	 * Constructeur par défaut du Job.
	 */
	public Job() throws RemoteException {
		super();
	}
	
	public Job(MapReduce mapReduce) throws RemoteException {
		this();
		this.mapReduce = mapReduce;
	}

	synchronized public void addDaemon(IDaemon d) throws RemoteException {
		System.out.println("NOUVEAU Daemon : "+d.getHostname());
		this.daemons.put(d.getHostname(), d);
	}

	@Override
	public void setNumberOfReduces(int tasks) {
		this.numberOfReduces = tasks;
		
	}

	@Override
	public void setNumberOfMaps(int tasks) {
		this.numberOfMaps = tasks;
		
	}

	@Override
	public void setInputFormat(Type ft) {
		this.inputFormat = ft;
		
	}

	@Override
	public void setOutputFormat(Type ft) {
		this.outputFormat = ft;
		
	}

	@Override
	public void setInputFname(String fname) {
		this.inputFname = fname;
	}

	@Override
	public void setOutputFname(String fname) {
		this.outputFname = fname;
		
	}

	@Override
	public int getNumberOfReduces() {
		return this.numberOfReduces;
	}

	@Override
	public int getNumberOfMaps() {
		return this.numberOfMaps;
	}

	@Override
	public Type getInputFormat() {
		return this.inputFormat;
	}

	@Override
	public Type getOutputFormat() {
		return this.outputFormat;
	}

	@Override
	public String getInputFname() {
		return this.inputFname;
	}

	@Override
	public String getOutputFname() {
		return this.outputFname;
	}
	
	public void setHeartBeatReceiver(HeartBeatReceiver hb) {
		this.hb = hb;
	}
	
	public HeartBeatReceiver getHeartBeatReceiver() {
		return this.hb;
	}
	
	public HashMap<String, IDaemon> getDaemons() {
		return daemons;
	}

	public void setDaemons(HashMap<String, IDaemon> daemons) {
		this.daemons = daemons;
	}

	public MapReduce getMapReduce() {
		return mapReduce;
	}

	public void setMapReduce(MapReduce mapReduce) {
		this.mapReduce = mapReduce;
	}

	/*
	 * Lance le HeartBeatReceiver hb afin de surveiller l'état des daemons.
	 */
	public void startHeartBeat() {
		this.hb.start();
	}
	
	/* 
	 * Lance le job.
	 */
	public void startJob(MapReduce mr) {
		this.setMapReduce(mr);
		
		/*
		 * Lancement des tâches Map.
		 */
		
		// TODO : vérification du nombre de daemons disponibles
		
		// TODO : lancement des tâches map en quantité this.numberOfMaps, en parrallèle
		
		/*
		 * Récupération des clefs envoyées par les daemons (tâches Maps).
		 */
		
		/*
		 * Lancement des tâches Reduces.
		 */
		
		// TODO : vérification du nombre de daemons disponibles
		
		// TODO : lancement des tâches reduce en quantité this.numberOfReduces, en parrallèle
		
		/*
		 * Attribution des clefs à chaque serveur et envoie d'une HashMap à chaque daemon qui indique à quel 
		 * daemon envoyer chaque clef.
		 */
		
		// TODO : envoie des hashmaps<Key, Daemons> aux daemons
		
		// TODO : les daemons effectuent le reduce
		
		/*
		 * Récupération des réduces pour concaténation et écriture du résultat final.
		 */
		
	}
	
	public static void main(String args[]) {
		try {
			LocateRegistry.createRegistry(Job.portRMI);
			Job job = new Job();
			Naming.bind("//localhost:"+Job.portRMI+"/Launcher", job);
			job.setHeartBeatReceiver(new HeartBeatReceiver(job.getDaemons()));
			job.getHeartBeatReceiver().start();
			/*
			 * L'accès concurrent sur les daemons oblige l'utilisation d'un Iterator.
			 */
			Iterator<String> it_daemons;
			  while(true) {
			 	System.out.println("\n=====Daemons=====");
			 	it_daemons = job.getDaemons().keySet().iterator();
			 	for(String hst = it_daemons.next(); it_daemons.hasNext(); hst = it_daemons.next()) {
			 		System.out.println(hst);
			 	}
			 	TimeUnit.SECONDS.sleep(1);
			 }
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
