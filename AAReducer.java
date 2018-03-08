package spring2018.lab2;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;

public class AAReducer  extends Reducer <Text,Text,Text,Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
		   throws IOException, InterruptedException {
        
        // TODO: initialize integer sums for each reading frame
    	int countRF1 =0;
    	int countRF2 =0;
    	int countRF3 =0;
    	String seprateValue;
    	String keyNew = key.toString().trim();
    	
        
        // TODO: loop through Iterable values and increment sums for each reading frame
    	for(Text itr: values){
    		seprateValue = itr.toString();
    		if(seprateValue.equals("readFrame1"))
    			countRF1++;
    		if(seprateValue.equals("readFrame2"))
    			countRF2++;
    		if(seprateValue.equals("readFrame3"))
    			countRF3++;
    	}
       String output = new String("\t"+ countRF1 +"\t"+ countRF2 +"\t"+ countRF3);
        // TODO: write the (key, value) pair to the context
    	context.write(new Text(keyNew), new Text(output));
        
               
	  
   }
}
