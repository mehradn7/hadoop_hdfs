package hdfs;

public class HdfsCommandUnknown extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String msg;
	
	public String toString() {
		return this.msg;
	}
}
