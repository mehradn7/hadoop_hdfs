package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;

public class Launcher extends UnicastRemoteObject implements ILauncher {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected Launcher() throws RemoteException {
		super();
	}

	@Override
	synchronized public void addDaemon(IDaemon d) throws RemoteException {
		//System.out.println("NOUVEAU Daemon : "+d.getLocalHostname());
		Job.daemons.put(d.getLocalHostname(), d);
	}
	
	synchronized public int numberOfDaemons() throws RemoteException {
		return Job.daemons.keySet().size();
	}

	@Override
	public Collection<IDaemon> getDaemons() throws RemoteException {
		return new ArrayList<IDaemon>(Job.daemons.values());
	}

}
