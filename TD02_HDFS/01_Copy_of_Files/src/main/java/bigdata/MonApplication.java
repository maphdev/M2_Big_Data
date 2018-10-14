//=====================================================================
/**
* Squelette minimal d'une application Hadoop
* A exporter dans un jar sans les librairies externes
* A exécuter avec la commande ./hadoop jar NOMDUFICHER.jar ARGUMENTS....
*/
package bigdata;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class MonApplication {
	public static class MonProg extends Configured implements Tool {
		public int run(String[] args) throws Exception {
			
			// local path
			String localInputPath = args[0];

			// hdfs path
			URI uri = new URI(args[1]).normalize();
			Path hdfsOutputPath = new Path(uri.getPath());
			
			// create stream
			InputStream is = new BufferedInputStream(new FileInputStream(localInputPath));
			FileSystem fs = FileSystem.get(uri, getConf(), "training");
			OutputStream os = fs.create(hdfsOutputPath);
			
			// copy
			IOUtils.copyBytes(is,  os, getConf());
			
			// close stream
			is.close();
			os.close();
			
			return 0;
		}
	}
	public static void main( String[] args ) throws Exception {
		int returnCode = ToolRunner.run(new MonApplication.MonProg(), args);
		System.exit(returnCode);
	}
}
//=====================================================================

