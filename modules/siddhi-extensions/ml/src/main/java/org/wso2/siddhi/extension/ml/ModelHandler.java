/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.siddhi.extension.ml;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.wso2.siddhi.extension.ml.exceptions.SiddhiSparkMLExtException;

public class ModelHandler {

    private static final Log log = LogFactory.getLog(ModelHandler.class);
    public static final String FILE_STORAGE_PREFIX = "file";
    public static final String REGISTRY_STORAGE_PREFIX = "registry";
    public static final String PATH_TO_GOVERNANCE_REGISTRY = "/_system/governance";

    private PipelineModel mlModel;
    private final SparkSession spark;
    private StructType schema;
    private Encoder<?> outputDataTypeEncoder;

    /**
     *
     * @param modelStorageLocation MLModel storage location
     * @param schema schema of the feature set
     * @param outputDataTypeEncoder Encoder for the output data type
     */
    public ModelHandler(String modelStorageLocation, StructType schema, Encoder<?> outputDataTypeEncoder) {
        spark = SparkSession
                .builder()
                .appName("Spark Predictor")
                .master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        this.schema = schema;
        mlModel = PipelineModel.load(modelStorageLocation);
        this.outputDataTypeEncoder = outputDataTypeEncoder;
    }

    /**
     * Predict the value using the feature values.
     * @param data          feature values array
     * @return              predicted value
     * @throws SiddhiSparkMLExtException on a incompatible data type error
     */
    public Object predict(Object[] data) throws SiddhiSparkMLExtException {

        // SparkSession spark = SparkSession
        // .builder()
        // .appName("Spark Predictor")
        // .master("local")
        // .config("spark.some.config.option", "some-value")
        // .getOrCreate();
        StructType predictionSchema = null;
        try {
            // Prepare test documents.
            List<Row> dataSet = Arrays.asList(RowFactory.create(data));
            Dataset<Row> inputs = spark.createDataFrame(dataSet, schema);
            // spark.cr.createDataFrame(data, beanClass)
            // Dataset<String> inputs = spark.createDataset(Arrays.asList(data), stringEncoder);
            // System.out.println(inputs.collectAsList());
            Dataset<Row> predictions = mlModel.transform(inputs);
            predictions.show();
            predictionSchema = predictions.select("prediction").schema();
            return predictions.select("prediction").as(outputDataTypeEncoder).collectAsList().get(0);
        } catch (ClassCastException ex) {
            String msg = String.format("Incompatible output data type provided. Expected: %s, but %s found.",
                    predictionSchema == null ? "" : predictionSchema.typeName(), outputDataTypeEncoder.schema()
                            .typeName());
            throw new SiddhiSparkMLExtException(msg, ex);
        }

        // ArrayList<String[]> list = new ArrayList<String[]>();
        // list.add(data);
        // Predictor predictor = new Predictor(modelId, mlModel, list);
        // List<?> predictions = predictor.predict();
        // String predictionStr = predictions.get(0).toString();
        // Object prediction = castValue(outputType, predictionStr);
        // return prediction;
    }

//    /**
//     * Predict the value using the feature values.
//     * @param data feature values array
//     * @param percentile percentile value for predictions
//     * @return predicted value
//     * @throws MLModelHandlerException
//     */
//    public Object predict(String[] data, String outputType, double percentile) throws MLModelHandlerException {
//        ArrayList<String[]> list = new ArrayList<String[]>();
//        list.add(data);
//        Predictor predictor = new Predictor(modelId, mlModel, list, percentile, false);
//        List<?> predictions = predictor.predict();
//        String predictionStr = predictions.get(0).toString();
//        Object prediction = castValue(outputType, predictionStr);
//        return prediction;
//    }
//
//    /**
//     * Predict the value using the feature values with POJO predictor.
//     * @param data          feature values array
//     * @param outputType    data type of the output
//     * @param pojoPredictor POJO predictor
//     * @return              predicted value
//     * @throws              MLModelHandlerException
//     */
//    public Object predict(String[] data, String outputType, POJOPredictor pojoPredictor)
//            throws MLModelHandlerException {
//        String predictionStr = pojoPredictor.predict(data).toString();
//        Object prediction = castValue(outputType, predictionStr);
//        return prediction;
//    }
//
//    /**
//     * Cast the given value to the given output type.
//     * @param outputType Output data type
//     * @param value value to be casted in String
//     * @return Value casted to output type object
//     */
//    private Object castValue(String outputType, String value) {
//        if (outputType.equalsIgnoreCase("double")) {
//            return Double.parseDouble(value);
//        } else if (outputType.equalsIgnoreCase("float")) {
//            return Float.parseFloat(value);
//        } else if (outputType.equalsIgnoreCase("integer") || outputType.equalsIgnoreCase("int")) {
//            return Integer.parseInt(value);
//        } else if (outputType.equalsIgnoreCase("long")) {
//            return Long.parseLong(value);
//        } else if (outputType.equalsIgnoreCase("boolean") || outputType.equalsIgnoreCase("bool")) {
//            return Boolean.parseBoolean(value);
//        } else {
//            return value;
//        }
//    }
//
//    /**
//     * Return the map containing <feature-name, index> pairs
//     * @return the <feature-name, feature-index> map of the MLModel
//     */
//    public Map<String, Integer> getFeatures() {
//        List<Feature> features = mlModel.getFeatures();
//        Map<String, Integer> featureIndexMap = new HashMap<String, Integer>();
//        for(Feature feature : features) {
//            featureIndexMap.put(feature.getName(), feature.getIndex());
//        }
//        return featureIndexMap;
//    }
//
//    /**
//     * Get new to old indices list of this model.
//     * @return the new to old indices list of the MLModel
//     */
//    public List<Integer> getNewToOldIndicesList() {
//        return mlModel.getNewToOldIndicesList();
//    }
//
//    /**
//     * Return the response variable of the model
//     * @return the response variable of the MLModel
//     */
//    public String getResponseVariable() {
//        return mlModel.getResponseVariable();
//    }
//
//    /**
//     * Returns the algorithm class - classification, numerical prediction or clustering
//     * @return the algorithm class
//     */
//    public String getAlgorithmClass() {
//        return mlModel.getAlgorithmClass();
//    }
//
//    /**
//     * @return the model
//     */
//    public MLModel getMlModel() {
//        return mlModel;
//    }
}
