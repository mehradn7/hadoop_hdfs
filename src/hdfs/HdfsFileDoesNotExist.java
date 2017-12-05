package hdfs;

public class HdfsFileDoesNotExist extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String msg = "Hdfs file does not exist !!!";
	
	public String toString(){
		return this.msg;
	}
	
}
