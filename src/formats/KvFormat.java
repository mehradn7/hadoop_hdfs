package formats;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

public class KvFormat implements Format {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private BufferedReader br;
	private BufferedWriter bw;
	private Long index = (long) 0;
	private String Fname;
	
	public KvFormat(String fname){
		this.Fname = fname;
	}

	@Override
	public KV read() {
		String res;
		String[] parsing;
		KV kv = null;
		try {
			if ((res = br.readLine()) != null) {
				parsing = res.split(KV.SEPARATOR);
				kv = new KV(parsing[0], parsing[1]);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return kv;
	}

	@Override
	public void write(KV record) {
		try{
			bw.write(record.k+KV.SEPARATOR+record.v);
			bw.newLine();
			bw.flush();
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	@Override
	public void open(OpenMode mode) {
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
		return (this.index++);
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
