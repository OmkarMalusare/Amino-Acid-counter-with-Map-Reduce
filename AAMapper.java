package spring2018.lab2;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class AAMapper  extends Mapper <LongWritable,Text,Text,Text> {
    
    Map<String, String> codon2aaMap = new HashMap<String, String>();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try{
            Path[] codon2aaPath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if(codon2aaPath != null && codon2aaPath.length > 0) {
                codon2aaMap = readFile(codon2aaPath);
            }
            } catch(IOException ex) {
                System.err.println("Exception in mapper setup: " + ex.getMessage());
                System.exit(1);
            }
        }
    
    protected HashMap<String, String> readFile(Path[] codonFilePath) {
        HashMap<String, String> codonMap = new HashMap<String, String>();
        BufferedReader cacheReader=null;
        String line=null;
        String[] lineArray=null;
        try{
           cacheReader = new BufferedReader(new FileReader(codonFilePath[0].toString()));
           while((line=cacheReader.readLine())!=null) {
               // Isoleucine      I       ATT, ATC, ATA
                 lineArray = line.split("\\t");
                 String aminoAcid = lineArray[0];
                 String[] sequencesArray = lineArray[2].split(",");
                 for(String sequence: sequencesArray) {
                     codonMap.put(sequence.trim(), aminoAcid.trim());
                 }
           }
        }
        catch(Exception e) { 
            e.printStackTrace(); 
            System.exit(1);
        }
        return codonMap;
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, 
        InterruptedException {
        
    	String line = value.toString();
     
        
    	   String readFrame1, readFrame2, readFrame3;
    	   
        
        for(int i=0;i<line.length();i=i+3){
        	if(i+2<line.length()){
        	readFrame1 = line.substring(i, i+3);
        	if(codon2aaMap.containsKey(readFrame1))
        		context.write(new Text(codon2aaMap.get(readFrame1)), new Text("readFrame1"));
        }}

       
      
        for(int i=1;i<line.length();i=i+3){
        	if(i+2<line.length()){
        	readFrame2 = line.substring(i, i+3);
        	if(codon2aaMap.containsKey(readFrame2))
        		context.write(new Text(codon2aaMap.get(readFrame2)), new Text("readFrame2"));
        }}
        
    
        for(int i=2;i<line.length();i=i+3){
        	if(i+2<line.length()){
        	readFrame3 = line.substring(i, i+3);
        	if(codon2aaMap.containsKey(readFrame3))
        		context.write(new Text(codon2aaMap.get(readFrame3)), new Text("readFrame3"));
        }}
       
    }
}
