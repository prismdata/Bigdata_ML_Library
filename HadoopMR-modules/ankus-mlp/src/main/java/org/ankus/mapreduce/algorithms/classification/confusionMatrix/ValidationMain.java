/*
 * Copyright (C) 2011 ankus (http://www.openankus.org).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.ankus.mapreduce.algorithms.classification.confusionMatrix;

//import org.ankus.mapreduce.algorithms.classification.C45_num_Purity.C45FinalClassifyingMapper;
import org.ankus.util.Constants;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: moonie
 * Date: 14. 5. 20
 * Time: 오후 1:15
 * To change this template use File | Settings | File Templates.
 */
public class ValidationMain {
	private Logger logger = LoggerFactory.getLogger(ValidationMain.class);

    /**
     *
     * @param fs            target file system (for hadoop)
     * @param inputPath     confusion matrix row data (org-class, pred-class, frequency)
     * @param delimiter     input file delimiter
     * @param outputPath    final classification result (validation performance)
     * @throws Exception
     */
    public void validationGeneration(FileSystem fs, String inputPath, String delimiter, String outputPath) throws Exception
    {
        /**
         * 2. PostProcessing
         *      Reduce Result Read and Class Label Identification
         *      Make Result
         *          >> summary > total instances, correctly classified instances, incorrect ~
         *          >> confusion Matrix
         *          class, total(weighted) > TP rate, FP rate, Precision, Recall, F1-Measure
         **/

        ArrayList<String[]> readStrList = new ArrayList<String[]>();
        ArrayList<String> uniqClassList = new ArrayList<String>();
        int totalDataCnt = 0;
        int correctDataCnt = 0;
        int inCorrectDataCnt = 0;

        // multi input file check
        // in-memory load all-cross-count and uniq class list
        FileStatus[] status = fs.listStatus(new Path(inputPath));
        for(int i=0; i<status.length; i++)
        {
            Path fp = status[i].getPath();
            if(fp.getName().indexOf("part-")<0) continue;
            FSDataInputStream fin = fs.open(status[i].getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
            String readStr, tokens[];
            int value;
            while((readStr=br.readLine())!=null)
            {
                tokens = readStr.split(delimiter);               
                readStrList.add(tokens);
                if(!uniqClassList.contains(tokens[0])) 
            	{
                	uniqClassList.add(tokens[0].trim());
            	}
                value = Integer.parseInt(tokens[2]);
                totalDataCnt += value;
                if(tokens[0].trim().equals(tokens[1].trim())) correctDataCnt += value;
                else inCorrectDataCnt += value;
            }
            br.close();
            fin.close();
        }

        // confusion matrix
        int classCnt = uniqClassList.size();
        logger.info("Class cnt:" + classCnt);
        int confusionMatrix[][] = new int[classCnt+1][classCnt+1];
        for(String[] infoStr: readStrList)
        {
            int row = uniqClassList.indexOf(infoStr[0].trim());
            int col = uniqClassList.indexOf(infoStr[1].trim());
            int value = Integer.parseInt(infoStr[2]);
            confusionMatrix[row][col] = value;

            confusionMatrix[row][classCnt] += value;
            confusionMatrix[classCnt][col] += value;
        }

        // performance
        double tpRate[] = new double[classCnt + 1];
        double fpRate[] = new double[classCnt + 1];
        
        double tp[] = new double[classCnt + 1];
        double fp[] = new double[classCnt + 1];
       
        double tn[] = new double[classCnt + 1];
        double pn[] = new double[classCnt + 1];
        
        double precision[] = new double[classCnt + 1];
        double recall[] = new double[classCnt + 1];
        double f1measure[] = new double[classCnt + 1];

        for(int i=0; i<classCnt; i++)
        {
            if(confusionMatrix[i][i]==0)
            {
                tpRate[i] = 0;
                precision[i] = 0;
                recall[i] = 0;
                f1measure[i] = 0;
            }
            else
            {
                tpRate[i] = (double)confusionMatrix[i][i] / (double)confusionMatrix[i][classCnt];
                if(confusionMatrix[classCnt][i] == confusionMatrix[i][i]) fpRate[i] = 0;
                else
                {
                    fpRate[i] = ((double)confusionMatrix[classCnt][i] - (double)confusionMatrix[i][i])
                            / ((double)totalDataCnt - (double)confusionMatrix[i][classCnt]);
                }

                precision[i] = (double)confusionMatrix[i][i] / (double)confusionMatrix[classCnt][i];
                recall[i] = tpRate[i];
                f1measure[i] = 2 * (precision[i] * recall[i]) / (precision[i] + recall[i]);
            }
        }

        tpRate[classCnt] = 0;
        fpRate[classCnt] = 0;
        precision[classCnt] = 0;
        recall[classCnt] = 0;
        f1measure[classCnt] = 0;
        for(int i=0; i<classCnt; i++)
        {
            double divValue = (double)confusionMatrix[i][classCnt] / (double)totalDataCnt;
            tpRate[classCnt] += tpRate[i] * divValue;
            fpRate[classCnt] += fpRate[i] * divValue;
            precision[classCnt] += precision[i] * divValue;
            recall[classCnt] += recall[i] * divValue;
            f1measure[classCnt] += f1measure[i] * divValue;
        }


        // output file generation
        FSDataOutputStream fout = fs.create(new Path(outputPath), true);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));

        bw.write("# Total Summary" + "\n");
        System.out.println("# Total Summary");
        bw.write("Total Instances: " + totalDataCnt + "\n");
        System.out.println("Total Instances: " + totalDataCnt);
        double tmpVal = (double)correctDataCnt / (double)totalDataCnt * 100;
        String formatStr = String.format("%2.2f",tmpVal);
        bw.write("Correctly Classified Instances: " + correctDataCnt + "(" + formatStr + "%)" +"\n");
        System.out.println("Correctly Classified Instances: " + correctDataCnt + "(" + formatStr + "%)");
        tmpVal = (double)inCorrectDataCnt / (double)totalDataCnt * 100;
        formatStr = String.format("%2.2f",tmpVal);
        bw.write("Incorrectly Classified Instances: " + inCorrectDataCnt + "(" + formatStr + "%)" + "\n");
        System.out.println("Incorrectly Classified Instances: " + inCorrectDataCnt + "(" + formatStr + "%)" );
        bw.write("\n");

        bw.write("# Confusion Matrix" + "\n");
        System.out.println("# Confusion Matrix");
        bw.write("(Classified as)");
        System.out.println("(Classified as)");
        String uniqClass = "";
        for(String classStr: uniqClassList) 
    	{
        	bw.write("\t" + classStr);
        	uniqClass += "\t" + classStr;
    	}
        logger.info(uniqClass);
        bw.write("\t|\ttotal\t" + "\n");
        System.out.println("\t|\ttotal\t");
        uniqClass = "";
        for(int i=0; i<classCnt; i++)
        {
            bw.write(uniqClassList.get(i));
            uniqClass += uniqClassList.get(i);
            for(int j=0; j<classCnt; j++) 
        	{
            	bw.write("\t" + confusionMatrix[i][j]);
            	 uniqClass += "\t" + confusionMatrix[i][j];
        	}
            bw.write("\t|\t#" + confusionMatrix[i][classCnt] + "\n");
            uniqClass += "\t|\t" + confusionMatrix[i][classCnt] + "\n";
        }
        System.out.println(uniqClass);
        bw.write("total");
        System.out.println("total");
        uniqClass = "";
        for(int i=0; i<classCnt; i++)
    	{
        	bw.write("\t" + confusionMatrix[classCnt][i]);
        	uniqClass +="\t" +  confusionMatrix[classCnt][i];
    	}
        System.out.println(uniqClass);
        bw.write("\n");
        bw.write("\n");

        bw.write("# Detailed Accuracy" + "\n");
        System.out.println("# Detailed Accuracy");
        bw.write("Class\tTP_Rate\tFP_Rate\tPrecision\tRecall\tF-Measure\n");
        System.out.println("Class\tTP_Rate\tFP_Rate\tPrecision\tRecall\tF-Measure");
        for(int i=0; i<classCnt; i++)
        {
            bw.write(uniqClassList.get(i) + "\t");
            bw.write(String.format("%1.3f", tpRate[i]) + "\t");
            bw.write(String.format("%1.3f", fpRate[i]) + "\t");
            bw.write(String.format("%1.3f", precision[i]) + "\t");
            bw.write(String.format("%1.3f", recall[i]) + "\t");
            bw.write(String.format("%1.3f", f1measure[i]) + "\n");
            System.out.println(uniqClassList.get(i) + "\t"+ String.format("%1.3f", tpRate[i]) + "\t"+ String.format("%1.3f", fpRate[i])  + "\t"+ String.format("%1.3f", precision[i])  + "\t"+ String.format("%1.3f", recall[i])  + "\t"+ String.format("%1.3f", f1measure[i]) );
        }
        bw.write("Weig.Avg.\t");
        System.out.println("Weig.Avg.\t");
        bw.write(String.format("%1.3f", tpRate[classCnt]) + "\t");
        bw.write(String.format("%1.3f", fpRate[classCnt]) + "\t");
        bw.write(String.format("%1.3f", precision[classCnt]) + "\t");
        bw.write(String.format("%1.3f", recall[classCnt]) + "\t");
        bw.write(String.format("%1.3f", f1measure[classCnt]) + "\n");
        System.out.println(String.format("%1.3f", tpRate[classCnt])   + "\t" +String.format("%1.3f", fpRate[classCnt]) + "\t"+ String.format("%1.3f", precision[classCnt]) + "\t"+String.format("%1.3f", recall[classCnt]) + "\t"+String.format("%1.3f", f1measure[classCnt]));
        bw.close();
        fout.close();
    }
}
