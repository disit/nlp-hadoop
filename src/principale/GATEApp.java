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

import gate.*;
import gate.creole.ExecutionException;
import gate.creole.ResourceInstantiationException;
import gate.util.GateException;
import gate.util.persistence.PersistenceManager;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.MalformedURLException;
import java.util.HashSet;
import java.util.Iterator;

public class GATEApp {

	private CorpusController application;
    public Corpus corpus;
    

    /**
     * Init GATE appl
     *
     */
    public GATEApp(String gateHome) throws GateException, IOException {
        
        Gate.setGateHome(new File(gateHome + "/GATETreeTagger/GATE/"));
        Gate.setPluginsHome(new File(gateHome + "/GATETreeTagger/GATE/plugins/"));
        Gate.setSiteConfigFile(new File(gateHome + "/GATETreeTagger/GATE/gate.xml"));
        Gate.setUserConfigFile(new File(gateHome + "/GATETreeTagger/GATE/user-gate.xml"));
        Gate.init();
        URL applicationURL = new URL("file:" + new Path(gateHome + "/GATETreeTagger/GATE/esempio.xgapp").toString());
        application = (CorpusController) PersistenceManager.loadObjectFromUrl(applicationURL);
        corpus = Factory.newCorpus("Hadoop Corpus");
        application.setCorpus(corpus);
    }

    /**
     * Part-Of-Speech TAGGER of Input Text
     */
    public String POSKeywordsAnnotation(String url) throws ResourceInstantiationException, ExecutionException, MalformedURLException {
        //System.out.println(url + "\n");
    //    Document document = Factory.newDocument(new URL(url));

    	Document document = Factory.newDocument(url);
        annotateDocument(document);
        
        HashSet<String> typeSet = new HashSet<String>();
        typeSet.add("Token");
        typeSet.add("Keyphrase");
		
        AnnotationSet a1 = document.getAnnotations().get(typeSet);
        
        String str = "";

        Iterator<Annotation> itr = a1.iterator();
        while (itr.hasNext()){

            Annotation ann = itr.next();
            if(ann.getType().equals("Token")){
                str = str + "" + ann.getFeatures().get("string") + " " + ann.getFeatures().get("category") + "\n";
            }else{
                if(ann.getType().equals("Keyphrase")){
                    str = str + "" + ann.getFeatures().get("value") + " " + ann.getFeatures().get("kind") + "\n";
                }
            }       
        }
        Factory.deleteResource(document);
        return str;
    }
    
    

    @SuppressWarnings({})
    private Document annotateDocument(Document document) throws ResourceInstantiationException, ExecutionException {
        corpus.add(document);
        application.execute();
        corpus.clear();
        return document;
    }




    public void close() {
        Factory.deleteResource(corpus);
        Factory.deleteResource(application);
    }



    /**
     * Runs the GATE .xgapp application as input argument 
     *
     */
    public static void main(String[] args) throws Exception {
    	
        String gateHome = args[0];
        String doc = FileUtils.readFileToString(new File(args[1]));

        GATEApp gatePipeline = new GATEApp(gateHome);
        String ann = gatePipeline.POSKeywordsAnnotation(doc);
        // Annotations printed as standard output
        System.out.println(ann);
        gatePipeline.close();
        
    }
    
	
}
