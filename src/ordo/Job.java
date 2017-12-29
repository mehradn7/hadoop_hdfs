package ordo;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import formats.Format;
import formats.Format.Type;
import formats.LineFormat;
import formats.SocketLocalFormat;
import map.MapReduce;

public class Job extends UnicastRemoteObject implements ILauncher, IJob {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/*
	 * Nom de l'hôte du job.
	 */
	public String hostname;
	
	/*
	 * Port du registry RMI.
	 */
	public static final int portRegistryRMI = 5000; // TODO
	
	/*
	 * Port de reception des clefs connues par les tâches Maps. 
	 */
	public static final int portMapperKeys = 5001;
	
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
	 * Barrière des Mappers
	 */
	private CountDownLatch stopMappers;
	
	/*
	 * Constructeur par défaut du Job.
	 */
	public Job() throws RemoteException {
		super();
		try {
			this.hostname = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public Job(MapReduce mapReduce) throws RemoteException {
		this();
		this.mapReduce = mapReduce;
	}

	synchronized public void addDaemon(IDaemon d) throws RemoteException {
		System.out.println("NOUVEAU Daemon : "+d.getLocalHostname());
		this.daemons.put(d.getLocalHostname(), d);
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
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

	public CountDownLatch getStopMappers() {
		return stopMappers;
	}

	public void setStopMappers(CountDownLatch stopMappers) {
		this.stopMappers = stopMappers;
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
		int numberOfDaemons = this.daemons.size(); // TODO : on suppose qu'il n'y a pas de panne des daemons
		
		Iterator<IDaemon> it_daemons = this.getDaemons().values().iterator(); // modification concurrente possible
		IDaemon current_daemon;
		
		if (this.getNumberOfMaps() <= numberOfDaemons) {
			for(int i = 0; i < this.getNumberOfMaps() && it_daemons.hasNext(); i++) { // TODO : non-parallèle
				current_daemon = it_daemons.next();
				
				// lecture du fraguement hdfs local de même nom que le fichier global
				Format reader = new LineFormat(this.getInputFname());
				
				// écriture : envoie des clefs au Job, et maintient des résultats dans une HashMap locale
				Format writer = new SocketLocalFormat(this.getHostname(), Job.portMapperKeys);
				
				// callback : indique la terminaison d'une tâche Map
				try {
					ICallBack cb = new CallBackMap(this.getStopMappers());
					current_daemon.runMap(this.getMapReduce(), reader, writer, cb);
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		// TODO : lancement des tâches map en quantité this.numberOfMaps, en parrallèle
		
		/*
		 * Récupération des clefs envoyées par les daemons (tâches Maps).
		 */
		
		/*
		 * Lancement des tâches Reduces.
		 */
		
		// TODO : vérification du nombre de daemons disponibles
		numberOfDaemons = this.daemons.size(); // TODO : on suppose qu'il n'y a pas de panne des daemons
		
		it_daemons = this.getDaemons().values().iterator(); // modification concurrente possible
		
		if (this.getNumberOfReduces() <= numberOfDaemons) {
			for(int i = 0; i < this.getNumberOfReduces() && it_daemons.hasNext(); i++) { // TODO : non-parallèle
				current_daemon = it_daemons.next();
				
				// lecture du fraguement hdfs local de même nom que le fichier global
				// TODO : Format reader = new LineFormat(this.getInputFname());
				
				// écriture : envoie des clefs au Job, et maintient des résultats dans une HashMap locale
				// TODO : Format writer = new SocketLocalFormat(this.getHostname(), Job.portMapperKeys);
				
				// callback : indique la terminaison d'une tâche Map
				try {
					// TODO : ICallBack cb = new CallBackMap(this.getStopMappers());
					current_daemon.runMap(this.getMapReduce(), reader, writer, cb);
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
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
			LocateRegistry.createRegistry(Job.portRegistryRMI);
			Job job = new Job();
			Naming.bind("//localhost:"+Job.portRegistryRMI+"/Launcher", job);
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
