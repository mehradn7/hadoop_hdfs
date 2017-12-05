package ordo;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class RessourceManager extends UnicastRemoteObject implements ILauncher {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static HashMap<String, IDaemon> daemons = new HashMap<String, IDaemon>();
	private static int portRMI = 5000; // TODO
	private static HeartBeatReceiver hb;

	protected RessourceManager() throws RemoteException {
	}

	synchronized public void addDaemon(IDaemon d) throws RemoteException {
		System.out.println("NOUVEAU Daemon : "+d.getHostname());
		RessourceManager.daemons.put(d.getHostname(), d);
	}

	public static void main(String args[]) {
		try {
			LocateRegistry.createRegistry(RessourceManager.portRMI);
			Naming.bind("//localhost:"+RessourceManager.portRMI+"/Launcher",
					new RessourceManager());
			RessourceManager.hb = new HeartBeatReceiver(RessourceManager.daemons);
			RessourceManager.hb.start();
			while(true) {
				System.out.println();
				System.out.println("=====Daemons=====");
				for(String hst : RessourceManager.daemons.keySet()) {
					System.out.println(hst);
				}
				TimeUnit.SECONDS.sleep(1);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
