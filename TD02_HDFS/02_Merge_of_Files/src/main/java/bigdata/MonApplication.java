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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class MonApplication {
	public static class MonProg extends Configured implements Tool {
		public int run(String[] args) throws Exception {
			
			// output hdfs path
			URI uri = new URI(args[args.length -1]).normalize();
			Path hdfsOutputPath = new Path(uri.getPath());
			
			// open output stream
			FileSystem fs = FileSystem.get(uri, getConf(), "training");
			OutputStream os = fs.create(hdfsOutputPath);
			
			// open input streams, then copy it in output stream
			for(int i = 0; i < args.length -1; ++i){
				InputStream is = new BufferedInputStream(new FileInputStream(args[i]));
				IOUtils.copyBytes(is,  os,  getConf(), false);	// false : we don't want to close the is and os at the end
				is.close();
			}
			
			// close output stream
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

