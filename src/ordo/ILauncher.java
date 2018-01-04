package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashMap;

public interface ILauncher extends Remote {

	public void addDaemon(IDaemon d) throws RemoteException;
	
	public int numberOfDaemons() throws RemoteException;
	
	public Collection<IDaemon> getDaemons() throws RemoteException;
	
	public HashMap<String, IDaemon> getHashMapDaemons() throws RemoteException;
	
}