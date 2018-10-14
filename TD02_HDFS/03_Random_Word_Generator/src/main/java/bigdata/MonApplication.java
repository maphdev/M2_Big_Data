//=====================================================================
/**
* Squelette minimal d'une application Hadoop
* A exporter dans un jar sans les librairies externes
* A exécuter avec la commande ./hadoop jar NOMDUFICHER.jar ARGUMENTS....
*/
package bigdata;

import java.net.URI;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class MonApplication {
	public static class MonProg extends Configured implements Tool {
		public int run(String[] args) throws Exception {
			
			String[] syllables = {"ha", "ku", "na", "ma", "ta", "ta", "mais", "quel", "le", "phra", "se", "magn", "if", "ique", "ha", "ku", "na", "ma", "ta", "ta", "quel", "chant", "fan", "ta", "sti", "que", "ces", "mots", "si", "gni", "fient", "que", "tu", "vi", "vras", "ta", "vie", "sans", "au", "cun", "sou", "ci", "phi", "lo", "so", "phie", "ha", "ku", "na", "ma", "ta", "ta"};
				
			// number of syllables to append to the file
			int nbSyllables = Integer.parseInt(args[0]);
			
			// output hdfs path of the file on which we append syllables
			URI uri = new URI(args[1]).normalize();
			Path hdfsOutputPath = new Path(uri.getPath());
			
			// output stream
			FileSystem fs = FileSystem.get(uri, getConf(), "training");
			FSDataOutputStream os = fs.append(hdfsOutputPath);
			
			// add nbSyllables syllables
			for(int i = 0; i < nbSyllables; i++){
				// (int) Math.random() * max + min;
				// here min = 0 and max = the index of the array's last element
				int randomIndex = (int) (Math.random() * (syllables.length-1));
				os.writeBytes(syllables[randomIndex]);
			}
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

/*try {



} catch (Exception e) {
	System.out.println("Usage : prog nb_syllables hdfs_file");
	return 0;
}	*/	