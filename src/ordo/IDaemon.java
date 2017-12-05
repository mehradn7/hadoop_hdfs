package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

import formats.Format;
import map.Mapper;

public interface IDaemon extends Remote {

	public String getHostname() throws RemoteException;
	public void runMap (Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException;
}
