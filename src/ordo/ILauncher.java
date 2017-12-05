package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ILauncher extends Remote {

	public void addDaemon(IDaemon d) throws RemoteException;

}