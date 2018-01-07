package hdfs;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class NameNode implements Runnable {
	public static String hostname = "bobafett";
	public static int port = 8091;
	public static int heartBeatPort = 5002;
	private ServerSocket ss;
	private Socket s;
	private Map<String, Integer> availableServers = new LinkedHashMap<String, Integer>();
	private List<INode> listINodes = new ArrayList<INode>();

	@Override
	public void run() {

		/* Lancer un thread chargé de recevoir les heartBeats des serveurs */
		runHeartBeatReceiver();

		/* Ouvrir le ServerSocket */
		openServerSocket(port);

		while (true) {
			/* Recevoir une requête */
			try {
				s = ss.accept();
			} catch (IOException e) {
				e.printStackTrace();
			}

			/* Lancer un thread qui va traiter la requête */
			new Thread(new TraitantNameNode(s, this.availableServers, this.listINodes)).start();

		}
	}

	private void runHeartBeatReceiver() {
		try {
			new HeartBeatReceiverNameNode(this.availableServers).start();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	private void openServerSocket(int port) {
		try {
			ss = new ServerSocket(port);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
