package org.ankus.mapreduce.algorithms.utils.TF_IDF;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
/**
 * WordsInCorpusTFIDFReducer calculates the number of documents in corpus that a given key occurs and the TF-IDF computation.
 * The total number of D is acquired from the job name<img draggable="false" class="emoji" alt="🙂" src="https://s0.wp.com/wp-content/mu-plugins/wpcom-smileys/twemoji/2/svg/1f642.svg"> It is a dirty hack, but the only way I could communicate the number from
 * the driver.
 * @author Marcello de Sales (marcello.desales@gmail.com)
 */
public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
 
    private static final DecimalFormat DF = new DecimalFormat("###.####");
    MultipleOutputs<Text, Text> mos  = null;
    private Logger logger = LoggerFactory.getLogger(TFIDFReducer.class);
    Configuration conf = null;
    String output_path = "";
    public TFIDFReducer() {
    }
    protected void setup(Context context) throws IOException, InterruptedException
	{
		conf = context.getConfiguration();
		output_path = conf.get(ArgumentsConstants.OUTPUT_PATH)+"_Raw/";
		mos = new MultipleOutputs<Text, Text>(context);
		
	}
    /**
     * @param key is the key of the mapper
     * @param values are all the values aggregated during the mapping phase
     * @param context contains the context of the job run
     *
     *             PRECONDITION: receive a list of <word, ["doc1=n1/N1", "doc2=n2/N2"]>
     *             POSTCONDITION: <"word@doc1,  [d/D, n/N, TF-IDF]">
     */
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // get the number of documents indirectly from the file-system (stored in the job name on purpose)
        int numberOfDocumentsInCorpus = Integer.parseInt(context.getJobName());
        // total frequency of this word
        int numberOfDocumentsInCorpusWhereKeyAppears = 0;
        Map<String, String> tempFrequencies = new HashMap<String, String>();
        for (Text val : values) {
            String[] documentAndFrequencies = val.toString().split("=");
            numberOfDocumentsInCorpusWhereKeyAppears++;
            tempFrequencies.put(documentAndFrequencies[0], documentAndFrequencies[1]);
        }
        for (String document : tempFrequencies.keySet())
        {
            String[] wordFrequenceAndTotalWords = tempFrequencies.get(document).split("/"); 
 
            //Term frequency is the quocient of the number of terms in document and the total number of terms in doc
            double tf = Double.valueOf(Double.valueOf(wordFrequenceAndTotalWords[0])
                    / Double.valueOf(wordFrequenceAndTotalWords[1]));
 
            //interse document frequency quocient between the number of docs in corpus and number of docs the term appears
            double idf = (double) numberOfDocumentsInCorpus / (double) numberOfDocumentsInCorpusWhereKeyAppears;
 
            //given that log(10) = 0, just consider the term frequency in documents
            double tfIdf = numberOfDocumentsInCorpus == numberOfDocumentsInCorpusWhereKeyAppears ? tf : tf * Math.log10(idf);
            
            document = document.replace(":", "_");
            document = document.replace(" ", "_");
            String emit_key  = key + "," + document;
//         String emit_value_detail = "[" + numberOfDocumentsInCorpusWhereKeyAppears + "/"
//                    + numberOfDocumentsInCorpus + " , " + wordFrequenceAndTotalWords[0] + "/"
//                    + wordFrequenceAndTotalWords[1] + " , " + DF.format(tfIdf) + "]";
         
         String emit_value_detail = numberOfDocumentsInCorpusWhereKeyAppears + "/"
                 + numberOfDocumentsInCorpus + " , " + wordFrequenceAndTotalWords[0] + "/"
                 + wordFrequenceAndTotalWords[1] ;
         
            String emit_value = DF.format(tf)  + "," +DF.format(Math.log10(idf))+ "," + DF.format(tfIdf) + ","+emit_value_detail;
            System.out.println(emit_key +"\t" + emit_value );
            
            String path_mos = output_path + document ;
            mos.write(new Text(emit_key), new Text(emit_value), path_mos);
        }
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {

		mos.close();//Dynamic File Name provider stream close
		int mb = 1024*1024;
		Runtime runtime = Runtime.getRuntime();
		logger.info("Memoery:"+ (runtime.totalMemory() - runtime.freeMemory()));
        if((runtime.totalMemory() - runtime.freeMemory()) / mb > runtime.maxMemory() / mb)
		{
			System.gc ();
			System.runFinalization ();
		}
		System.out.println("cleanup");
    	
    }
}
