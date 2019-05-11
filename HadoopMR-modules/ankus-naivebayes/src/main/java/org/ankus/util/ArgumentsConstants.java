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

package org.ankus.util;

/**
 * ArgumentsConstants
 * @desc
 *      Collected constants of parameter by user
 * @version 0.0.1
 * @date : 2013.08.23
 * @author Suhyun Jeon
 * @author Moonie Song
 * @author HongJoong Shin
 */
public class ArgumentsConstants {

    // common
    public static final String INPUT_PATH = "-input";
    public static final String OUTPUT_PATH = "-output";
    public static final String LOCAL_LOG_PATH = "-logpath";
    public static final String KEEP_OUTPUT_PATH = "-keep_output";
    public static final String DELIMITER = "-delimiter";
    public static final String SUB_DELIMITER = "-subDelimiter";
    public static final String TARGET_INDEX = "-indexList";
    
    public static final String FILTER_TARGET_INDEX = "-filtertargetList";
    public static final String NOMINAL_INDEX = "-nominalIndexList";
    public static final String NUMERIC_INDEX = "-numericIndexList";
    public static final String EXCEPTION_INDEX = "-exceptionIndexList";
    public static final String MR_JOB_STEP = "-mrJobStep";
    public static final String TEMP_DELETE = "-tempDelete";
    public static final String HELP = "-help";          // current not used, for CLI
    
    // for certainty factor sum
    public static final String CERTAINTY_FACTOR_MAX = "-cfsumMax";

    // for normalization
    public static final String REMAIN_FIELDS = "-remainAllFields";
    public static final String DISCRETIZATION_COUNT = "-categoryCount";

    // for correlation and similarity
    public static final String KEY_INDEX = "-keyIndex";                     // contents based sim
    public static final String COMPUTE_INDEX = "-computeIndex";
    public static final String ALGORITHM_OPTION = "-algorithmOption";       // cf-sim, contents based sim

    // for decision tree ID3, C4.5
    public static final String RULE_PATH = "-ruleFilePath";
    public static final String CLASS_INDEX = "-classIndex";

    // for k-means, em
    public static final String NORMALIZE = "-normalize";
    public static final String MAX_ITERATION = "-maxIteration";
    public static final String CLUSTER_COUNT = "-clusterCnt";
    public static final String CLUSTER_PATH = "-clusterPath";
    public static final String CLUSTER_TRAINING_CONVERGE = "-convergeRate";

    //MR-DBSCAN
    public static final String EPSILON = "-epsilon";
    public static final String MINPTS = "-MinPts";
    
    // for canopy
    public static final String CANOPY_T1 = "-t1";
    public static final String CANOPY_T2 = "-t2";
    
    //for Fuzzy-C-Means
    public static final String Fuzzy_CMeans_K = "-k"; //클러스터 수 
    public static final String Fuzzy_CMeans_P = "-p"; //가중치
    public static final String Fuzzy_CMeans_MAXITERATION = "-maxIteration"; //가중치

    // for classification and clustering
    public static final String DISTANCE_OPTION = "-distanceOption";
    public static final String FINAL_RESULT_GENERATION = "-finalResultGen";
    public static final String TRAINED_MODEL = "-modelPath";

    // for MultilayerPercept
    public static final String SEED = "-seed";
    public static final String HIDDEN_NODE_NUM = "-numHiddenNodes";
    public static final String LEARNING_RATE = "-learningRate";
    public static final String MOMENTUN = "-momentum";
    public static final String END_CONDITION = "-endCondition";
    public static final String MAX_EPOCH = "-maxEpoch";
    public static final String MAX_ERROR = "-maxError";
    public static final String SUB_MAX_EPOCH = "-subMaxEpoch";
    public static final String NOMALIZE_TYPE = "-nomalizeType";
    public static final String MIN_LIST = "-minList";
    public static final String MAX_LIST = "-maxList";
    public static final String NUMERIC_LIST = "-numericList";
    public static final String NOMINAL_LIST = "-nominalList";
    public static final String TYPE_LIST = "-typeList";
    public static final String NORMAL_NOMINAL_KEY_LIST = "-norNominalKeyList";
    public static final String CLASS_NOMINAL_KEY_LIST = "-classNominalKeyList";
    public static final String INPUT_NODE_NUM = "-numInputNodes";
    public static final String OUTPUT_NODE_NUM = "-numOutputNodes";
    public static final String CLASS_INDEX_TYPE = "-classIdxType";
    public static final String HIDDEN_WEIGHT = "-hiddenWeihht";
    public static final String OUTPUT_WEIGHT = "-outputWeight";
    public static final String CLASS_MAX = "-classMax";
    public static final String CLASS_MIN = "-classMin";
    public static final String TEST_DATA = "-testPath";
    public static final String WEIGHT_PATH = "-weightPath";
    
    // for recomendation
    public static final String COMMON_COUNT = "-commonCount";
    public static final String UID_INDEX = "-uidIndex";
    public static final String IID_INDEX = "-iidIndex";
    public static final String RATING_INDEX = "-ratingIndex";
    public static final String BASED_TYPE = "-basedType";
    public static final String TARGET_ID = "-targetID";
    public static final String CORRVALLIMIT = "-corrValLimit";
    public static final String SUMMATION_OPTION = "-sumOption";


    public static final String SIMILARITY_DELIMITER = "-similDelimiter";
    public static final String SIMILARITY_PATH = "-similPath";
    public static final String SIMILARITY_THRESHOLD = "-similThreshold";
    public static final String RECOMMENDATION_CNT = "-recomCnt";
    public static final String TARGET_UID = "-targetUID";
    public static final String TARGET_IID_LIST = "-targetIIDList";

    
    public static final String USER_INDEX = "-userIndex";
    public static final String ITEM_INDEX = "-itemIndex";
    public static final String THRESHOLD = "-threshold";
    public static final String SIMILARITY_DATA_INPUT = "-similarDataInput";
    public static final String RECOMMENDED_DATA_INPUT = "-recommendedDataInput";


    /*
     TODO: if new variables are added
     TODO: then, must update "ConfigurationVariable" class for input arguments setting
     */
    //to etl filter 
    public static final String ETL_T_METHOD = "-etlMethod";
    public static final String ETL_RULE_PATH = "-filterRulePath";
    public static final String ETL_RULE = "-filterRule";
    public static final String ETL_FILTER_COLUMNS = "-columnsList";
    
    public static final String ETL_REPLACE_RULE = "-ReplaceRule";
    public static final String ETL_REPLACE_RULE_PATH = "-ReplaceRulePath";
    
    public static final String ETL_NUMERIC_NORM = "-NumericForm"; //Method
    
    public static final String ETL_NUMERIC_NORM_RULE_PATH = "-NumericFormFile"; 
    
    public static final String ETL_NUMERIC_SORT_METHOD = "-Sort";
    
    public static final String ETL_NUMERIC_SORT_TARGET = "-SortTarget";
  
    //TF-IDF Option
    //TF-IDF Option
    public static final String TF_IDF_DOWN_LIMIT = "-downLimit";
    public static final String TF_IDF_ONE_FILE_PATH = "-integrationPath";
    public static final String TF_IDF_KEYWORD_PATH = "-keywordPath"; 
    public static final String TF_IDF_NUMREDUCER = "-numReducer";
    public static final String TF_IDF_TERM_FREQ = "-termFreq";
    
    //Keyword Simailarity
    public static final String DOC_ID_POSITION= "-docidIndex";
    
    public static final String CLASS_LIST = "-classList";
    
}
