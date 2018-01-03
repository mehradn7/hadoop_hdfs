package hdfs;

import java.io.IOException;

public class TestSplitFile {

	public static void main(String[] args) {
		try {
			HdfsUtil.splitFile("testfichier", 10);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
