package ordo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;

import formats.Format;
import formats.Format.OpenMode;
import formats.KV;
import formats.KvFormat;
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
	public static final int portReducersKeys = 5003;
	
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
	
	/*
	 * Nom du fichier résultat du mapper.
	 */
	private String mapperFname;
	
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
		this.setMapperFname(writer.getFname());

		/*
		 * On crée un thread esclave qui va exécuter le map.
		 */
		MapSlave mapperSlave = new MapSlave(reader, writer, mapper, callbackMapper);
		mapperSlave.start();
	}
	
	public void runReducer (Reducer reducer, Format reader, Format writer, ICallBack callbackReducer)
			throws RemoteException {
		reader.setFname(Daemon.prefix + reader.getFname());
		writer.setFname(Daemon.prefix + writer.getFname());
		
		/*
		 * On crée un thread esclave qui va exécuter le reduce
		 */
		ReducerSlave reducerSlave = new ReducerSlave(reader, writer, reducer, callbackReducer, this);
		reducerSlave.start();
	}
	
	public void runReceiver(Format writer, ICallBack callbackReceiver) throws RemoteException {
		writer.setFname(Daemon.prefix + writer.getFname());
		ReceiveReduce receiveReduce = new ReceiveReduce(writer, callbackReceiver);
		receiveReduce.start();
	}
	
	public void runSender() throws RemoteException {
		SendReduce sendReduce = new SendReduce(this);
		sendReduce.start();
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

	public String getMapperFname() throws RemoteException {
		return mapperFname;
	}

	public void setMapperFname(String mapperFname) throws RemoteException {
		this.mapperFname = mapperFname;
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
class ReducerSlave extends Thread {

	private Format reader;
	private Format writer;
	private Reducer reducer;
	private ICallBack callback;
	private IDaemon daemon;


	public ReducerSlave(Format reader, Format writer, Reducer reducer, ICallBack callback, IDaemon daemon) {
		this.reader = reader;
		this.writer = writer;
		this.reducer = reducer;
		this.callback = callback;
		this.daemon = daemon;
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
		System.out.println("Reducer terminé.");

	}
}

/**
 * Classe chargée d'envoyer les KVs du fraguement local aux reducers correspondants.
 */
class SendReduce extends Thread {

	private IDaemon daemon;
	
	public SendReduce(IDaemon daemon) {
		this.daemon = daemon;
	}
	
	public void run() {
		try {
			HashMap<String, String> keyToDaemon = this.daemon.getKeyToDaemon();
			HashMap<String, ObjectOutputStream> ipToOos = new HashMap<String, ObjectOutputStream>();
			String ip_current_daemon;
			Format reader = new KvFormat(this.daemon.getMapperFname());
			reader.open(OpenMode.R);
			KV kv;
			
			/*
			 * Envoie des KV au reducer correspondant.
			 */
			while ((kv = reader.read()) != null) {
				ip_current_daemon = keyToDaemon.get(kv.k);
				if (!ipToOos.containsKey(ip_current_daemon)) {
					ipToOos.put(ip_current_daemon, new ObjectOutputStream(
							(new Socket(ip_current_daemon, Daemon.portReducersKeys)).getOutputStream()));
				}
				ipToOos.get(ip_current_daemon).writeObject(kv);
			}
			
			/*
			 * Fermeture des sockets.
			 */
			for(ObjectOutputStream oos : ipToOos.values()) {
				oos.close();
			}
			
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

class ReceiveReduce extends Thread {
	
	private ServerSocket ss;
	private Format writer;
	private ICallBack callbackReceiver;
	
	public ReceiveReduce(Format writer, ICallBack callbackReceiver) {
		try {
			this.ss = new ServerSocket(Daemon.portReducersKeys);
			this.ss.setSoTimeout(100);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.writer = writer;
		this.callbackReceiver = callbackReceiver;
	}

	public void run() {
		Socket s;
		ObjectInputStream ois;
		KV kv;
		this.writer.open(OpenMode.W);
		while(true) {
			try {
				s = this.ss.accept();
				ois = new ObjectInputStream(s.getInputStream());
				while((kv = (KV) ois.readObject()) != null) {
					this.writer.write(kv);
				}
			} catch (SocketTimeoutException e) {
				try {
					this.ss.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				System.out.println("Fermeture du receiver !");
				return;
			} catch (IOException e) {
				try {
					this.callbackReceiver.isTerminated();
				} catch (RemoteException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				this.writer.close();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
