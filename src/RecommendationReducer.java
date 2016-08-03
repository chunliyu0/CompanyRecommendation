/**
 * RecommendationReducer.java
 * CIS 612 - Cloud Computing Spr.16
 * Created on April 10, 2016, 5:33:40 PM
 * @author Chunli Yu
 */

package org.sample;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class RecommendationReducer extends Reducer<Text,Text,Text,Text> {
	HashMap<String, Integer> recommendationMap;
	String [] recommendationPair;
	List<String> inputVals;
	StringBuffer allRecommendations = new StringBuffer();
	
	public void reduce(Text key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {
		recommendationMap = new HashMap<String, Integer>();
		allRecommendations = new StringBuffer(); 
		inputVals = new ArrayList<String>();
		for (Text val : values) 
			inputVals.add(val.toString());
		
		//Store all <recommendation, connectivity> in hashmap based on the key (a company)
		for (String val : inputVals) {
			recommendationPair = val.toString().split(",");
			//it will overwrite the corresponding recommendation with the value -1
			if(val.contains("-1")) 
				recommendationMap.put(recommendationPair[0], new Integer(-1));
			
			if(recommendationMap.containsKey(recommendationPair[0])){
				//if it's not been assigned with -1 before，add it to hashmap
				if (recommendationMap.get( recommendationPair[0]) != -1) 
					recommendationMap.put(recommendationPair[0],recommendationMap.get(recommendationPair[0]) + 1);
			}
			else 
				recommendationMap.put(recommendationPair[0],1);
		}
		
		//Sort the recommendationMap based on the the companyId as well as connectivity
        List<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(recommendationMap.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
            	if(o2.getValue().compareTo(o1.getValue())==0)
            		return o1.getKey().compareTo(o2.getKey());
                return o2.getValue().compareTo(o1.getValue());
            }
        });  
        
        //Sum up the recommendations with connectivity
        for(Entry<String, Integer> entry: list){
        	if(entry.getValue()==-1) break;//-1's are filtered to the end of the list
        	allRecommendations.append("["+entry.getKey() + ": " + entry.getValue()+"] ");
        }   
		context.write(new Text(allRecommendations.toString()), new Text("=>"+key.toString()));
	}
}
