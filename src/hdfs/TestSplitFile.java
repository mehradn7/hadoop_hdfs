package hdfs;

import java.io.IOException;

public class TestSplitFile {

	public static void main(String[] args) {
		try {
			HdfsUtil.splitFile(args[0], 10);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
