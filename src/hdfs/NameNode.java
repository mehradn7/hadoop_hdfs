package hdfs;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class NameNode implements Runnable {
	public static String hostname = "yoda"; /* TODO : namenode.txt */
	public static int port = 8091;
	private ServerSocket ss;
	private Socket s;
	private Map<String, Integer> availableServers = new LinkedHashMap<String, Integer>();
	private List<INode> listINodes = new ArrayList<INode>();

	@Override
	public void run() {

		try {
			ss = new ServerSocket(port);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		while (true) {
			/* Recevoir une requête */
			try {
				s = ss.accept();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			new Thread(new TraitantNameNode(s, this.availableServers, this.listINodes)).start();
			
		}
	}

	
}
