package ordo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import formats.Format;
import formats.Format.Type;
import formats.KvFormat;
import formats.LineFormat;
import formats.SocketAndKvFormat;
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
	 * Adresse IP de l'hôte.
	 */
	public static final String inetAddress = "192.168.1.14";
	
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
		Job.hb.start();
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
		ILauncher launcher = null;
		try {
			launcher = (ILauncher) Naming.lookup("//"+Job.inetAddress+":"+Job.portRegistryRMI+"/Launcher");
		} catch (MalformedURLException | RemoteException | NotBoundException e2) {
			e2.printStackTrace();
		}
		int numberOfDaemons = 0;
		try {
			numberOfDaemons = launcher.numberOfDaemons();
		} catch (RemoteException e2) {
			e2.printStackTrace();
		} // TODO : on suppose qu'il n'y a pas de panne des daemons
		System.out.println("Nombre de Daemons connectés : "+numberOfDaemons);
		this.setNumberOfMaps(numberOfDaemons); // TODO : fixe pour le moment
		
		
		Collection<IDaemon> daemons = null;
		try {
			daemons = launcher.getDaemons();
		} catch (RemoteException e2) {
			e2.printStackTrace();
			return;
		} // modification concurrente possible
		Iterator<IDaemon> it_daemons = daemons.iterator();
		IDaemon current_daemon;
		
		/*
		 * Récupération des clefs envoyées par les daemons (tâches Maps).
		 */
		KeysReceiverSlave keyReceiver = new KeysReceiverSlave(this);
		keyReceiver.start();
		
		/*
		 * Lancement des tâches map en quantité this.numberOfMaps, en parrallèle TODO
		 */
		if (this.getNumberOfMaps() <= numberOfDaemons) {
			
			this.setBarrierForMappers(new CountDownLatch(this.getNumberOfMaps())); // création barrière
			
			for(int i = 0; i < this.getNumberOfMaps(); i++) { // TODO : non-parallèle
				System.out.println("Lancement du Mapper : "+(i+1));
				
				current_daemon = it_daemons.next();
				
				// lecture du fraguement hdfs local de même nom que le fichier global
				Format reader = new LineFormat(this.getInputFname());
				
				// écriture : envoie des clefs au Job, et maintient des résultats dans une HashMap locale
				Format writer = new SocketAndKvFormat(this.getInputFname()+"-mapper");
				
				try {
					// callback : indique la terminaison d'une tâche Map
					ICallBack callbackMapper = new CallBackMap(this.getBarrierForMappers());
					
					// lancement du mapper sur le daemon courrant
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
			System.out.println("En attente de la terminaison des mappers...");
			this.getBarrierForMappers().await();
			keyReceiver.join();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		
		/*
		 * On récupère l'ensemble des clefs envoyées par les mappers.
		 */
		Set<String> keys = keyReceiver.getKeys();
		System.out.println("Clefs réceptionnées : "+keys);
				
		/*
		 * Lancement des tâches Reduces.
		 */
		
		// Préparation pour attribution des clefs aux différentes machines
		try {
			numberOfDaemons = launcher.numberOfDaemons();
		} catch (RemoteException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} // TODO : on suppose qu'il n'y a pas de panne des daemons
		this.setNumberOfReduces(numberOfDaemons); // TODO : temporaire
		
		int numberOfKeys = keys.size(); // nombre de clefs reçues
		int nbOfKeysByDaemon =  (int) (numberOfKeys/numberOfDaemons) ; // nombre de clefs par Daemon pour Reduce
		HashMap<String, String> keyToDaemon = new HashMap<String, String>();
		
		try {
			it_daemons = launcher.getDaemons().iterator();
		} catch (RemoteException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} // modification concurrente possible
		Iterator<String> it_keys = keys.iterator();
		
		if (this.getNumberOfReduces() <= numberOfDaemons) {
			
			/*
			 * Répartition des clefs.
			 */
			System.out.println("ICI");
			for(int i = 0; (i < this.getNumberOfReduces()); i++) {
				current_daemon = it_daemons.next();
				for(int j = 0; (j < nbOfKeysByDaemon) && (it_keys.hasNext()); j++) {
					try {
						keyToDaemon.put(it_keys.next(), current_daemon.getLocalHostname());
					} catch (RemoteException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					it_keys.remove();
				}
			}
			
			System.out.println("KeyToDaemon : "+keyToDaemon);
			
			if (true) {
				return; // TEMPORAIRE
			}
			
			try {
				it_daemons = launcher.getDaemons().iterator();
			} catch (RemoteException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} // modification concurrente possible
			
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
		System.out.println("[] : Lancement du Ressource Manager...");
		try {
			/*
			 * Création d'un RMI registry et enregistrement du Job.
			 */
			LocateRegistry.createRegistry(Job.portRegistryRMI);
			System.out.println("[] : RMI registry OK");
			Launcher launcher = new Launcher();
			Naming.bind("//localhost:"+Job.portRegistryRMI+"/Launcher", launcher);
			System.out.println("[] : Launcher OK");
			
			/*
			 * Création d'un HeartBeatReceiver pour le job et lancement du HeartBeat.
			 */
			Job.hb = new HeartBeatReceiver(Job.daemons);
			Job.hb.start();
			System.out.println("[] : HeartBeat OK");
			
			/*
			 * L'accès concurrent sur les daemons oblige l'utilisation d'un Iterator.
			 */
			System.out.println("[] : En attente de Daemons");
			int previousSize, diff;
			int currentSize = 0;
			String hst;
			Iterator<String> it_daemons;
			  while(true) {
			 	it_daemons = Job.daemons.keySet().iterator();
			 	previousSize = currentSize;
				currentSize = Job.daemons.size();
				diff = currentSize - previousSize;
				if (diff != 0) {
					System.out.println("\n[] : Daemons ->");
				 	while(it_daemons.hasNext()) {
				 		hst = it_daemons.next();
				 		System.out.println(" - "+hst);
				 	}
				}
			 	TimeUnit.SECONDS.sleep(1);
			 }
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}

/**
 * 
 * Classe résponsalbe de la réception des clefs envoyées par les mappers au serveur.
 * 
 */
class KeysReceiverSlave extends Thread {
	
	/*
	 * Poignée du job en cours.
	 */
	private Job job;
	
	/*
	 * Ensemble des clefs collectés.
	 */
	Set<String> keys;
	
	public KeysReceiverSlave(Job job) {
		this.job = job;
		this.keys = new HashSet<String>();
	}
	
	public Job getJob() {
		return job;
	}

	public void setJob(Job job) {
		this.job = job;
	}

	public Set<String> getKeys() {
		return keys;
	}

	public void setKeys(Set<String> keys) {
		this.keys = keys;
	}

	@SuppressWarnings("unchecked")
	public void run() {
		/*
		 * Récupération des clefs envoyées par les daemons (tâches Maps).
		 */
		try {
			ServerSocket serverSocketForMappersKeys = new ServerSocket(Job.portMapperKeys);
			
			ArrayList<Socket> sockets = new ArrayList<Socket>();
			while (sockets.size() != this.job.getNumberOfMaps()) {
				sockets.add(serverSocketForMappersKeys.accept());
			}
			ObjectInputStream ois;
			Set<String> receivedKeys;
			for(Socket s : sockets) {
				ois = new ObjectInputStream(s.getInputStream());
				
				receivedKeys = (HashSet<String>) ois.readObject();
				this.keys.addAll(receivedKeys);
				
				ois.close();
				s.close();
			}
			
			serverSocketForMappersKeys.close();
		} catch (IOException | ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

}
