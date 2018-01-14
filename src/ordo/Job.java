package ordo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
import hdfs.HdfsClient;
import hdfs.HdfsUtil;
import hdfs.INode;
import hdfs.NameNode;
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
	public static final String inetAddress = "147.127.133.174";
	
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
	private CountDownLatch barrierForReducers;
	
	/*
	 * Barrière des Receivers.
	 */
	private CountDownLatch barrierForReceivers;
	
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

	public CountDownLatch getBarrierForReducers() {
		return barrierForReducers;
	}

	public void setBarrierForReducers(CountDownLatch barrierForReducers) {
		this.barrierForReducers = barrierForReducers;
	}

	public CountDownLatch getBarrierForReceivers() {
		return barrierForReceivers;
	}

	public void setBarrierForReceivers(CountDownLatch barrierForReceivers) {
		this.barrierForReceivers = barrierForReceivers;
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
		 * Répartition du fichier source.
		 */
		//HDFS - begin
		try {
			HdfsClient.HdfsWrite(Type.LINE, this.getInputFname(), 1);
		} catch (ClassNotFoundException | IOException | InterruptedException e3) {
			e3.printStackTrace();
		}
		
		ArrayList<INode> listeFichiers = null;
		INode inode = null;
		try {
			listeFichiers = HdfsUtil.getListINodes();
		} catch (ClassNotFoundException | IOException e3) {
			// TODO Auto-generated catch block
			e3.printStackTrace();
		}
		for (INode in : listeFichiers) {
			if (in.getFilename().equals(this.getInputFname())) {
				inode = in;
				break;
			}
		}
		HashMap<Integer, ArrayList<String>> mapBlocs = inode.getMapBlocs();
		this.setNumberOfMaps(mapBlocs.size());
		// HDFS - end
		
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
		
		//this.setNumberOfMaps(numberOfDaemons); // TODO : fixe pour le moment
		
		
		HashMap<String, IDaemon> daemons = null;
		try {
			daemons = launcher.getHashMapDaemons();
		} catch (RemoteException e2) {
			e2.printStackTrace();
			return;
		} // modification concurrente possible
		//Iterator<IDaemon> it_daemons = daemons.iterator();
		Iterator<IDaemon> it_daemons = null;
		IDaemon current_daemon;

		/*
		 * Récupération des clefs envoyées par les daemons (tâches Maps).
		 */
		KeysReceiverSlave keyReceiver = new KeysReceiverSlave(this);
		keyReceiver.start();
		
		/*
		 * Lancement des tâches map en quantité this.numberOfMaps, en parrallèle TODO
		 */
			
		this.setBarrierForMappers(new CountDownLatch(this.getNumberOfMaps())); // création barrière
		
		for(int i = 1; i <= mapBlocs.size(); i++) { // TODO : non-parallèle
			System.out.println("Lancement du Mapper : "+(i+1));
			
			current_daemon = daemons.get(mapBlocs.get(i).get(0));
			
			// lecture du fraguement hdfs local de même nom que le fichier global
			Format reader = new LineFormat(this.getInputFname()+i);
			
			// écriture : envoie des clefs au Job, et maintient des résultats dans une HashMap locale
			Format writer = new SocketAndKvFormat(this.getInputFname()+i+"-mapper");
			
			try {
				// callback : indique la terminaison d'une tâche Map
				ICallBack callbackMapper = new CallBackMap(this.getBarrierForMappers());
				
				// lancement du mapper sur le daemon courrant
				current_daemon.runMap(this.getMapReduce(), reader, writer, callbackMapper);
			} catch (RemoteException e) {
				e.printStackTrace();
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
		if(true) {
			return;
		}
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
			current_daemon = null;
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
			while(it_keys.hasNext()) {
				try {
					keyToDaemon.put(it_keys.next(), current_daemon.getLocalHostname());
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			System.out.println("KeyToDaemon : " + keyToDaemon);
			
			try {
				it_daemons = launcher.getDaemons().iterator(); // choix des daemons pour reduces
				for(int i = 0; i < this.getNumberOfReduces(); i++) {
					it_daemons.next().setKeyToDaemon(keyToDaemon);
				}
			} catch (RemoteException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			
			/*
			 * Lancement des receivers.
			 */
			
			this.setBarrierForReceivers(new CountDownLatch(this.getNumberOfReduces()));
			
			try {
				it_daemons = launcher.getDaemons().iterator();
			} catch (RemoteException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} // modification concurrente possible
			
			for(int i = 0; i < this.getNumberOfReduces(); i++) { // TODO : non-parallèle
				current_daemon = it_daemons.next();
				
				// lecture de ce qu'envoient les autres daemons
				Format writer = new KvFormat(this.getInputFname()+"-reducerIN");
				
				// callback : indique la terminaison d'un reducer
				try {
					ICallBack callbackReceiver = new CallBackReceiver(this.getBarrierForReceivers());
					current_daemon.runReceiver(writer, callbackReceiver);
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			/*
			 * Lancement des senders.
			 */
			
			try {
				it_daemons = launcher.getDaemons().iterator();
			} catch (RemoteException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} // modification concurrente possible
			
			for(int i = 0; i < this.getNumberOfMaps(); i++) { // TODO : non-parallèle
				current_daemon = it_daemons.next();
				try {
					current_daemon.runSender();
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			// on attend les receivers
			try {
				System.out.println("En attente des receivers...");
				this.getBarrierForReceivers().await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			/*
			 * Lancement des reducers.
			 */
			
			this.setBarrierForReducers(new CountDownLatch(this.getNumberOfReduces())); // création barrière pour reducers
			
			//HDFS - begin
			mapBlocs = new HashMap<Integer, ArrayList<String>>();
			ArrayList<String> ips;
			//HDFS - end
			
			try {
				it_daemons = launcher.getDaemons().iterator();
			} catch (RemoteException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} // modification concurrente possible
			
			for(int i = 0; i < this.getNumberOfReduces(); i++) { // TODO : non-parallèle
				current_daemon = it_daemons.next();
				
				// lecture de ce qu'envoient les autres daemons
				Format reader = new KvFormat(this.getInputFname()+"-reducerIN");
				
				// écriture : locale pour le moment
				Format writer = new KvFormat(this.getInputFname()+"-reducerOUT"+(i+1)); // TODO : temporaire
				
				
				//HDFS - begin
				ips = (new ArrayList<String>());
				try {
					ips.add(current_daemon.getLocalHostname());
					mapBlocs.put(i+1, ips);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				//HDFS - end
				
				// callback : indique la terminaison d'un reducer
				try {
					ICallBack callbackReduce = new CallBackReduce(this.getBarrierForReducers());
					current_daemon.runReducer(this.getMapReduce(), reader, writer, callbackReduce);
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			//HDFS - begin
			try {
				Socket toNameNode;
				ObjectOutputStream oosNN;
				toNameNode = new Socket(NameNode.hostname, NameNode.port);
				oosNN = (new ObjectOutputStream(toNameNode.getOutputStream()));
				oosNN.writeObject("register");
				oosNN.writeObject(new INode(this.getInputFname()+"-reducerOUT", 1, mapBlocs));
				oosNN.close();
				toNameNode.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			//HDFS - end
			
		}// end if
		
		try {
			System.out.println("En attente des reducers...");
			this.getBarrierForReducers().await(); // on attend la fin des reducers.
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/*
		 * Récupération des réduces pour concaténation et écriture du résultat final.
		 */
		
		//HDFS - begin
		try {
			HdfsClient.HdfsRead(this.getInputFname()+"-reducerOUT", this.getOutputFname());
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//HDFS - end
		
		
	}
	
	public static void main(String args[]) {
		System.out.println("[] : Lancement du Ressource Manager...");
		try {
			System.out.println("[] : Host = "+InetAddress.getLocalHost().getHostAddress());
		} catch (Exception e) {
		}
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
					System.out.println("\n[] : Daemons connectés :");
				 	while(it_daemons.hasNext()) {
				 		hst = it_daemons.next();
				 		System.out.println("   IP = "+hst);
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
