package hdfs;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class RunNameNode {

	public static void main(String[] args) {
		NameNode namenode = new NameNode();
		try {
			NameNode.hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		namenode.run();
	}

}
