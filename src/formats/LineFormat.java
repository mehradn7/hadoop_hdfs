package formats;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

public class LineFormat implements Format {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private BufferedReader br;
	private BufferedWriter bw;
	private Long index = 1L; //Ligne lue lors du prochain read
	private String Fname;
	
	public LineFormat(String fname){
		this.Fname = fname;
	}

	@Override
	public KV read() {
		String res;
		KV kv = null;
		try {
			if ((res = br.readLine()) != null) {
				kv = new KV(String.valueOf(this.getIndex()), res);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.index++;
		return kv;
	}

	@Override
	public void write(KV record) {
		try{
			bw.write(record.v);
			bw.newLine();
			bw.flush();
		}catch(IOException e){
			e.printStackTrace();
		}
		this.index++;
	}

	@Override
	public void open(OpenMode mode) {
		this.index = 1L;
		if (mode == OpenMode.R){
			try {
				br = Files.newBufferedReader(Paths.get(getFname()), Charset.forName("UTF-8"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}else{
			try {
				bw = Files.newBufferedWriter(Paths.get(getFname()), Charset.forName("UTF-8"));
			} catch (IOException e) {
				e.printStackTrace();
			}		
		}
	}

	@Override
	public void close() {
		if (br != null){
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}else{
			try {
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}
	}

	@Override
	public long getIndex() {
		return this.index;
	}

	@Override
	public String getFname() {
		return this.Fname;
	}

	@Override
	public void setFname(String fname) {
		this.Fname = fname;
	}
}
