package tuffy.main;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import scala.Tuple2;
import tuffy.learn.DNLearner;
import tuffy.learn.MultiCoreSGDLearner;
import tuffy.parse.CommandOptions;
import tuffy.util.Config;
import tuffy.util.UIMan;
/**
 * The Main.
 */
public class MLNmain {
	
	public static ArrayList<Tuple2<String, Double>> main(String[] args) throws SQLException, IOException {

		ArrayList<Tuple2<String, Double>> attributes = null;
		//BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		/*String[] str = null;
		try {
			str = br.readLine().split(" ");
		} catch (Exception e) {
			 e.printStackTrace();
		}*/
		//CommandOptions options = UIMan.parseCommand(str);

		CommandOptions options = UIMan.parseCommand(args);
		
		UIMan.println("+++++++++ This is " + Config.product_name + "! +++++++++");
		if(options == null){
			return null;
		}
		if(!options.isDLearningMode){
			// INFERENCE
			if(!options.disablePartition){
				new PartInfer().run(options);
			}else{
				new NonPartInfer().run(options);
			}
		}else{
			if(options.mle){
				//SGDLearner l = new SGDLearner();
				MultiCoreSGDLearner l = new MultiCoreSGDLearner();
				l.run(options);
				l.cleanUp();
				
			}else{
				//LEARNING
				DNLearner l = new DNLearner();
				attributes = l.run(options);
			}
		}

		return attributes;
	}
}