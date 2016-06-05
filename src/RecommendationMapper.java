/**
 * RecommendationMapper.java
 * CIS 612 - Cloud Computing Spr.16
 * Instructor: Professor Jian Tang
 * Created on April 10, 2016, 5:00:00 PM
 * @author Chunli Yu [SUID: 422888242]
 */

package org.sample;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class RecommendationMapper extends Mapper<LongWritable, Text, Text, Text>{
	Text keyNode = new Text();
	Text directHop = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String [] line = value.toString().split(":");
		if (line.length==1)
			return;
		String [] recommendationList = line[1].split(",");
		for (int i=0; i<recommendationList.length; i++) {
			keyNode.set(new Text(recommendationList[i]));
			for(int j=0; j<recommendationList.length; j++){
				if(j==i) continue;//Ignore if the recommendation is itself
				//Emit <recommendationList[i], recommendationList[j], connectivity=1>
				context.write(keyNode, new Text(recommendationList[j] + ",1"));
			}
			directHop.set(line[0] + ",-1");//-1 indicates that they are directly connected
			//Emit <recommendationList[i], line[0], connectivity=-1> for all keyNode.
			context.write(keyNode, directHop);
		}
	}
}