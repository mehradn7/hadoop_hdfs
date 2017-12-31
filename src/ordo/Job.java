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
import formats.KvFormat;
import formats.LineFormat;
import formats.SocketFormat;
import map.MapReduce;

public class Job extends UnicastRemoteObject implements IJob {

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
	public static HashMap<String, IDaemon> daemons = new HashMap<String, IDaemon>();
	
	/*
	 * Poignée du HeartBeatReceiver qui surveille l'état des daemons de la HashMap "daemons"
	 * et qui met à jours cette HashMap en fonction de celui-ci.
	 */
	public static HeartBeatReceiver hb;
	
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
	 * Barrière des Mappers.
	 */
	private CountDownLatch barrierForMappers;
	
	/*
	 * Barrière des Reduces.
	 */
	private CountDownLatch barrierForReduces;
	
	/*
	 * Constructeur par défaut du Job.
	 */
	public Job() throws RemoteException {
		super();
		try {
			this.hostname = InetAddress.getLocalHost().getHostAddress();
			Job.daemons = new HashMap<String, IDaemon>();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public Job(MapReduce mapReduce) throws RemoteException {
		this();
		this.mapReduce = mapReduce;
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

	public MapReduce getMapReduce() {
		return mapReduce;
	}

	public void setMapReduce(MapReduce mapReduce) {
		this.mapReduce = mapReduce;
	}

	public CountDownLatch getBarrierForMappers() {
		return barrierForMappers;
	}

	public void setBarrierForMappers(CountDownLatch barrierForMappers) {
		this.barrierForMappers = barrierForMappers;
	}

	public CountDownLatch getBarrierForReduces() {
		return barrierForReduces;
	}

	public void setBarrierForReduces(CountDownLatch barrierForReduces) {
		this.barrierForReduces = barrierForReduces;
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
		
		// Vérification du nombre de daemons disponibles
		int numberOfDaemons = Job.daemons.size(); // TODO : on suppose qu'il n'y a pas de panne des daemons
		
		Iterator<IDaemon> it_daemons = Job.daemons.values().iterator(); // modification concurrente possible
		IDaemon current_daemon;
		
		if (this.getNumberOfMaps() <= numberOfDaemons) {
			
			// Lancement des tâches map en quantité this.numberOfMaps, en parrallèle
			for(int i = 0; i < this.getNumberOfMaps() && it_daemons.hasNext(); i++) { // TODO : non-parallèle
				current_daemon = it_daemons.next();
				
				// lecture du fraguement hdfs local de même nom que le fichier global
				Format reader = new LineFormat(this.getInputFname());
				
				// écriture : envoie des clefs au Job, et maintient des résultats dans une HashMap locale
				// TODO : Format writer = new SocketLocalFormat(this.getHostname(), Job.portMapperKeys);
				Format writer = new KvFormat(this.getInputFname()); 
				
				// callback : indique la terminaison d'une tâche Map
				try {
					ICallBack callbackMapper = new CallBackMap(this.getBarrierForMappers());
					current_daemon.runMap(this.getMapReduce(), reader, writer, callbackMapper);
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			}
		}
		
		/*
		 * On attend la terminaison de toutes les tâches Map.
		 */
		try {
			this.getBarrierForMappers().await();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		
		if (true) {
			return; // TEMPORAIRE
		}
		
		// TODO : lancement des tâches map en quantité this.numberOfMaps, en parrallèle
		
		/*
		 * Récupération des clefs envoyées par les daemons (tâches Maps).
		 */
		
		// TODO :
		
		/*
		 * Lancement des tâches Reduces.
		 */
		
		// TODO : vérification du nombre de daemons disponibles
		numberOfDaemons = Job.daemons.size(); // TODO : on suppose qu'il n'y a pas de panne des daemons
		
		it_daemons = Job.daemons.values().iterator(); // modification concurrente possible
		
		if (this.getNumberOfReduces() <= numberOfDaemons) {
			for(int i = 0; i < this.getNumberOfReduces() && it_daemons.hasNext(); i++) { // TODO : non-parallèle
				current_daemon = it_daemons.next();
				
				// lecture de ce qu'envoient les autres daemons
				Format reader = new SocketFormat(Job.portMapperKeys);
				
				// écriture : locale pour le moment
				Format writer = new KvFormat(this.getInputFname()); // TODO : temporaire
				
				// callback : indique la terminaison d'une tâche Map
				try {
					ICallBack callbackReduce = new CallBackReduce(this.getBarrierForReduces());
					current_daemon.runReduce(this.getMapReduce(), reader, writer, callbackReduce);
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
			/*
			 * Création d'un RMI registry et enregistrement du Job.
			 */
			LocateRegistry.createRegistry(Job.portRegistryRMI);
			Launcher launcher = new Launcher();
			Naming.bind("//localhost:"+Job.portRegistryRMI+"/Launcher", launcher);
			
			/*
			 * Création d'un HeartBeatReceiver pour le job et lancement du HeartBeat.
			 */
			Job.hb = new HeartBeatReceiver(Job.daemons);
			Job.hb.start();
			
			/*
			 * L'accès concurrent sur les daemons oblige l'utilisation d'un Iterator.
			 */
			Iterator<String> it_daemons;
			
			  while(true) {
			 	System.out.println("\n=====Daemons=====");
			 	it_daemons = Job.daemons.keySet().iterator();
			 	
			 	String hst;
			 	while(it_daemons.hasNext()) {
			 		hst = it_daemons.next();
			 		System.out.println(hst);
			 	}
			 	TimeUnit.SECONDS.sleep(1);
			 }
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
