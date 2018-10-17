package caoss.simulator;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;

public class MemUtil {
	
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("error");
		}
		
		BufferedReader in = new BufferedReader(new FileReader(args[0]));
		PrintStream out = new PrintStream(new FileOutputStream(args[1]));
		
		try {
			while (true) {
				String data = in.readLine();
				String[] access = data.split(" ");
				char type = access[1].charAt(0);
				
				if (type == 'R')
					out.println("load  0x" + access[0]);
				else
					out.println("store 0x" + access[0]);
			}
		}
		catch (Exception e) {
			in.close();
			out.close();
	}
}

}
