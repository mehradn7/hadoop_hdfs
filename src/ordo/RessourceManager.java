package ordo;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class RessourceManager extends UnicastRemoteObject implements ILauncher {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static Collection<IDaemon> daemons = new ArrayList<IDaemon>();
	private static int portRM = 5000; // TODO
	private static HeartBeatReceiver hb;

	protected RessourceManager() throws RemoteException {
		super();
	}

	@Override
	synchronized public void addDaemon(IDaemon d) throws RemoteException {
		System.out.println("NOUVEAU Daemon : "+d.getHostname());
		daemons.add(d);
	}

	public static void main(String args[]) {
		try {
			LocateRegistry.createRegistry(RessourceManager.portRM);
			Naming.bind("//localhost:"+RessourceManager.portRM+"/Launcher",
					new RessourceManager());
			RessourceManager.hb = new HeartBeatReceiver(RessourceManager.daemons);
			RessourceManager.hb.start();
			while(true) {
				System.out.println();
				System.out.println("=====Daemons=====");
				for(IDaemon d : daemons) {
					System.out.println(d.getHostname());
				}
				TimeUnit.SECONDS.sleep(10);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
