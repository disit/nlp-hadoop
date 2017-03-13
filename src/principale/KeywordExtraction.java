/* Nlp-Hadoop
   Copyright (C) 2017 DISIT Lab http://www.disit.org - University of Florence

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as
   published by the Free Software Foundation, either version 3 of the
   License, or (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>. */


package principale;

import java.io.IOException;

import java.text.SimpleDateFormat;
import java.net.MalformedURLException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import gate.util.GateException;
import gate.creole.ResourceInstantiationException;
import gate.creole.ExecutionException;



public class KeywordExtraction {
	
	// User-Defined Value for TF-IDF Thresholding
	public static double thres = .8;
                                                            
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		
		public String getHost(String url){
		    if(url == null || url.length() == 0)
		        return "";

		    int doubleslash = url.indexOf("//");
		    if(doubleslash == -1)
		        doubleslash = 0;
		    else
		        doubleslash += 2;

		    int end = url.indexOf('/', doubleslash);
		    end = end >= 0 ? end : url.length();

		    int port = url.indexOf(':', doubleslash);
		    end = (port > 0 && port < end) ? port : end;

		    return url.substring(doubleslash, end);
		}
		

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			try
		     {

				String line = value.toString();

				String domainString = " ";
				String parsedText = " ";
				if (line.contains(" TEXT:: ")) {
					domainString = getHost(line.split(" TEXT:: ")[0].split("URL:: ")[1]);

					parsedText = " ";
					if (!line.endsWith(" TEXT:: ")) {
						parsedText = line.split(" TEXT:: ")[1];
					} else {

					}
					
				} else {
					domainString = line;
					parsedText = " ";

					
				}
				

		            context.write(new Text(domainString),new Text(line));
		        	
		    //    }
		     } catch(IOException e)
		     {
		        e.printStackTrace();
		     } catch(InterruptedException e)
		     {
		        e.printStackTrace();
		     }
			
                        
		}
	}
		                                                                                  
	public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {
		
        private static GATEApp gate;
	  
	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	       if (null == gate) {
                  
                  Configuration c = context.getConfiguration();
                  Path[] localCache = DistributedCache.getLocalCacheArchives(c);
                  
	          try {
	             gate = new GATEApp(localCache[0].toString());
	          } catch (GateException e) {
	             throw new RuntimeException(e);
	          }
	       }
	    }
		                                    
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
                
			ArrayList<String[]> termsDocsArray = new ArrayList<String[]>();
	        ArrayList<HashMap<String, String>> POSTermsDocsArray = new ArrayList<HashMap<String, String>>();
			ArrayList<String> fileNames = new ArrayList<String>();
			ArrayList<String> allTerms = new ArrayList<String>();
			ArrayList<double[]> tfidfDocsVector = new ArrayList<double[]>();
		
                for(Text v: values){
                    	
                	String line = v.toString();
                
                	String urlString = " ";
    				String parsedText = " ";
    				if (line.contains(" TEXT:: ")) {
    					urlString = line.split(" TEXT:: ")[0].split("URL:: ")[1];
    				//	System.out.println("domain: " + urlString);
    					parsedText = " ";
    					if (!line.endsWith(" TEXT:: ")) {
    						parsedText = line.split(" TEXT:: ")[1];
    					} else {
    				//		String parsedText = " ";
    					}
    			//		System.out.println("Parsed Text: " + parsedText);
    				} else {
    					urlString = line;
    					parsedText = " ";
    					
    				}

                    String POSKeywords;
                    HashMap<String, String> POSTerms = new HashMap();
                    
                        try{
                        
                        //	System.out.println("");
                            POSKeywords = gate.POSKeywordsAnnotation(parsedText);
 
                            SimpleDateFormat sdf = new SimpleDateFormat();
                            sdf.applyPattern("yyyy-MM-dd");
                    
                       //     String dataStr = sdf.format(new Date());
                    
                            String[] lines = POSKeywords.split(System.getProperty("line.separator"));
                            String[] terms = new String[lines.length];
                       //     String[] urls = new String[lines.length];
                            for(int i = 0; i<lines.length; i++){
                                if(lines[i].contains("KPH")){
                                    String[] keyphrase = lines[i].split(" KPH");
                                    terms[i] = keyphrase[0];
                                    POSTerms.put(keyphrase[0], "KPH");
                                }else{
                                    if(lines[i].contains("NOM") || lines[i].contains("ADJ")){
                                        String[] keyword = lines[i].split(" ");
                                        terms[i] = keyword[0];
                                        POSTerms.put(keyword[0], keyword[1]);
                                    }
                                }
                                
                                
                            }
                            
                            for(String t : terms){
                                if(!allTerms.contains(t)){
                                    allTerms.add(t);
                                }
                            }
				
                            termsDocsArray.add(terms);
                            POSTermsDocsArray.add(POSTerms);
                            fileNames.add(urlString);	
                        
                            
                        } catch (ResourceInstantiationException e) {
                            System.out.println(urlString + " resource instantiation exception");
                        } catch (ExecutionException e) {
                            System.out.println(urlString + " execution exception" + e + "\n"); 
                            gate.corpus.clear();
                            System.out.println(urlString + " malformed URL exception");
                        } catch (IOException e){
                            System.out.println(urlString + " io exception" + e + "\n");
                            gate.corpus.clear();
                        }
                }	
		
               
               // TF-IDF Calculation------------------------------------------------------ 
                for (String[] docTermsArray : termsDocsArray) {
                    double[] tfidfvectors = new double[allTerms.size()];
                    int count = 0;
                    for (String tm : allTerms) {
                    	double tf = new TfIdf().tfCalculator(docTermsArray, tm);
                    	double idf = new TfIdf().idfCalculator(termsDocsArray, tm);
                    	double tfidf = tf * idf;
                    	tfidfvectors[count] = tfidf;
                    	count++;
                    }
                    tfidfDocsVector.add(tfidfvectors);     
                }
                //------------------------------------------------------------------------
			
                Iterator<double[]> itr = tfidfDocsVector.iterator();
                Iterator<String> itr2 = fileNames.iterator();
                Iterator<HashMap<String, String>> itr3 = POSTermsDocsArray.iterator();
                while(itr.hasNext()){
                	
                	ArrayList<Integer> topTfIdfTermsIndex = new ArrayList<Integer>();
                    double[] d = itr.next();
                    String fileName = itr2.next();
                    HashMap<String, String> POSterms = itr3.next();
				
                    for(int i = 0; i<d.length; i++){
                    	
                    	if (d[i] > thres) {
                    		topTfIdfTermsIndex.add(i);
                    	}
					
                    }
                  
                    String strout = "" + fileName;

                    for(int i = 0; i<topTfIdfTermsIndex.size(); i++) {
                    	strout = strout + ", " + allTerms.get(topTfIdfTermsIndex.get(i)) + ", " + POSterms.get(allTerms.get(topTfIdfTermsIndex.get(i)));
                    }
                    
                    SimpleDateFormat sdf = new SimpleDateFormat();
                    sdf.applyPattern("yyyy-MM-dd");
                    
                    String dataStr = sdf.format(new Date());
                    
                    strout = strout + ", " + dataStr + "";
                    
                    System.out.println(strout+"\n");
                    
                    Text out = new Text();
                    
                    out.set(strout);
                    
                    context.write(out, NullWritable.get());
                }
			
			
            }
		
            @Override
            protected void cleanup(Context context) throws IOException, InterruptedException {
                gate.close();
            }
        }

	public static void main(String[] args) throws Exception {
            
            String inputPath = "";
            String outputFolder = "";
            Configuration conf = new Configuration();
            
            conf.set("mapred.job.priority", JobPriority.VERY_HIGH.toString());
	
            // Path to Input Zipped GATE-TreeTagger Application
            Path localGateTreeTaggerApp = new Path("/home/hduser/Gate-App.zip");
            
            // Path to HDFS Destination of Input Zipped Application
            Path hdfsGateTreeTaggerApp = new Path("/tmp/Gate-app.zip");
            
            Job job = new Job(conf);
            
            job.setJarByClass(KeywordExtraction.class);
                    
            job.setJobName("Keyword Extraction");
				
            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);
            
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            
            job.setOutputKeyClass(Text.class);    
            job.setOutputValueClass(NullWritable.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);     

            if (args.length > 0) {
            	
            	// Path to Input Text File
            	Path inputFile = new Path("/home/hduser/inputTextFile.txt"); 
            	// HDFS Destination Path of Input Text File 
                Path hdfsInputFile = new Path("/tmp/inputTextFile.txt");
                   
                FileSystem fs = FileSystem.get(conf);
                fs.copyFromLocalFile(localGateTreeTaggerApp, hdfsGateTreeTaggerApp);
                   
                DistributedCache.addCacheArchive(hdfsGateTreeTaggerApp.toUri(), conf);
                   
                fs.copyFromLocalFile(inputFile, hdfsInputFile);
                DistributedCache.addCacheArchive(hdfsInputFile.toUri(), conf);
            	TextInputFormat.addInputPath(job, new Path("/tmp/inputFile.txt"));
            	
            }
            
            if (args.length > 1) {
            	// Define output folder;
            	outputFolder = "mnt/bigdsk/new_data/";
            	TextOutputFormat.setOutputPath(job, new Path(outputFolder + args[1] + ""));//output_" + System.currentTimeMillis() + ""));
            } else {
            	TextOutputFormat.setOutputPath(job, new Path(outputFolder));//output_" + System.currentTimeMillis() + ""));
            }
            
            boolean success = job.waitForCompletion(true);
            
            if (success){
                
                FileSystem.get(job.getConfiguration()).deleteOnExit(hdfsGateTreeTaggerApp);
            
            }
                
            System.exit(success ? 0 : -1);
	}
}
