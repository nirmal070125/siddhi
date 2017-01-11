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

import java.util.*;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

public class PredictStreamProcessor extends StreamProcessor {

    private ModelHandler modelHandler;
    private boolean attributeSelectionAvailable;

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

        while (streamEventChunk.hasNext()) {

            StreamEvent event = streamEventChunk.next();
            Object[] data = event.getOutputData();
            Object[] featureValues = Arrays.copyOfRange(data, 0, data.length-1);
//            int i=0;
//            String[] featureValues = new String[inputData.length];
//            for (Object object : inputData) {
//                featureValues[i] = String.valueOf(object);
//                i++;
//            }

            if (featureValues != null) {
                try {
                     Object prediction = modelHandler.predict(featureValues);
//                    if (AlgorithmType.CLASSIFICATION.getValue().equals(algorithmClass)) {
//                        for (int i = 0; i < modelHandlers.length; i++) {
//                            predictionResults[i] = modelHandlers[i].predict(featureValues, outputType);
//                        }
//                        // Gets the majority vote
//                        predictionResult = ObjectUtils.mode(predictionResults);
//                    } else if (AlgorithmType.NUMERICAL_PREDICTION.getValue().equals(algorithmClass)) {
//                        double sum = 0;
//                        for (int i = 0; i < modelHandlers.length; i++) {
//                            sum += Double.parseDouble(modelHandlers[i].predict(featureValues, outputType).toString());
//                        }
//                        // Gets the average value of predictions
//                        predictionResult = sum / modelHandlers.length;
//                    } else if (AlgorithmType.ANOMALY_DETECTION.getValue().equals(algorithmClass)) {
//                        for (int i = 0; i < modelHandlers.length; i++) {
//                            predictionResults[i] = modelHandlers[i].predict(featureValues, outputType, percentileValue);
//                        }
//                        // Gets the majority vote
//                        predictionResult = ObjectUtils.mode(predictionResults);
//
//                    } else if (AlgorithmType.DEEPLEARNING.getValue().equals(algorithmClass)) {
//                        // if H2O cluster is not available
//                        if (deeplearningWithoutH2O) {
//                            for (int i = 0; i < modelHandlers.length; i++) {
//                                predictionResults[i] = modelHandlers[i].predict(featureValues, outputType,
//                                        pojoPredictor[i]);
//                            }
//                            // Gets the majority vote
//                            predictionResult = ObjectUtils.mode(predictionResults);
//                        } else {
//                            for (int i = 0; i < modelHandlers.length; i++) {
//                                predictionResults[i] = modelHandlers[i].predict(featureValues, outputType);
//                            }
//                            // Gets the majority vote
//                            predictionResult = ObjectUtils.mode(predictionResults);
//                        }
//                    } else {
//                        String msg = String.format(
//                                "Error while predicting. Prediction is not supported for the algorithm class %s. ",
//                                algorithmClass);
//                        throw new ExecutionPlanRuntimeException(msg);
//                    }

                    complexEventPopulater.populateComplexEvent(event, new Object[]{prediction});
                } catch (Exception e) {
                    log.error("Error while predicting", e);
                    throw new ExecutionPlanRuntimeException("Error while predicting", e);
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {

        if (attributeExpressionExecutors.length < 1) {
            throw new ExecutionPlanValidationException("ML model storage location has not "
                    + "been defined as the first parameter");
        } else if (attributeExpressionExecutors.length == 1) {
            attributeSelectionAvailable = false; // model-storage-location
        } else {
            attributeSelectionAvailable = true; // model-storage-location, stream-attributes list
        }

        // model-storage-location
        String modelStorageLocation;
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            Object constantObj = ((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue();
            modelStorageLocation = (String) constantObj;
        } else {
            throw new ExecutionPlanValidationException(
                    "ML model storage-location has not been defined as the first parameter");
        }

        // data-type
        Encoder<?> outputDatatypeEncoder = Encoders.STRING();
        if (attributeExpressionExecutors.length > 1) {
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                Object constantObj = ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
                String outputType = (String) constantObj;
                outputDatatypeEncoder = getEncoder(outputType);
            }
        }
        StructType schema = buildSchema(inputDefinition.getAttributeList());
        
        modelHandler = new ModelHandler(modelStorageLocation, schema, outputDatatypeEncoder);
//
//        // Validate algorithm classes
//        // All models should have the same algorithm class.
//        // When put into a set, the size of the set should be 1
//        HashSet<String> algorithmClasses = new HashSet<String>();
//        for (int i = 0; i < modelHandlers.length; i++) {
//            algorithmClasses.add(modelHandlers[i].getAlgorithmClass());
//        }
//        if (algorithmClasses.size() > 1) {
//            throw new ExecutionPlanRuntimeException("Algorithm classes are not equal");
//        }
//        algorithmClass = modelHandlers[0].getAlgorithmClass();
//
//        // Validate features
//        // All models should have same features.
//        // When put into a set, the size of the set should be 1
//        HashSet<Map<String, Integer>> features = new HashSet<Map<String, Integer>>();
//        for (int i = 0; i < modelHandlers.length; i++) {
//            features.add(modelHandlers[i].getFeatures());
//        }
//        if (features.size() > 1) {
//            throw new ExecutionPlanRuntimeException("Features in models are not equal");
//        }
//
//        if (AlgorithmType.ANOMALY_DETECTION.getValue().equals(algorithmClass)) {
//            isAnomalyDetection = true;
//        }
//
//        if (!isAnomalyDetection) {
//            // Validate response variables
//            // All models should have the same response variable.
//            // When put into a set, the size of the set should be 1
//            HashSet<String> responseVariables = new HashSet<String>();
//            for (int i = 0; i < modelStorageLocations.length; i++) {
//                responseVariables.add(modelHandlers[i].getResponseVariable());
//            }
//            if (responseVariables.size() > 1) {
//                throw new ExecutionPlanCreationException("Response variables of models are not equal");
//            }
//            responseVariable = modelHandlers[0].getResponseVariable();
//
//        } else {
//            if (attributeExpressionExecutors.length == 3) {
//                attributeSelectionAvailable = false; // model-storage-location, data-type
//            } else {
//                attributeSelectionAvailable = true; // model-storage-location, data-type, stream-attributes list
//            }
//
//            // checking the percentile value
//            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
//                Object constantObj = ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
//                percentileValue = (Double) constantObj;
//
//            } else {
//                throw new ExecutionPlanValidationException(
//                        "percentile value has not been defined as the third parameter");
//            }
//
//            return Arrays.asList(new Attribute(anomalyPrediction, outputDatatype));
//        }
//        
//        if (AlgorithmType.DEEPLEARNING.getValue().equals(algorithmClass)) {
//            if (modelHandlers[0].getMlModel().getModel() != null) {
//                deeplearningWithoutH2O = false;
//            } else {
//                deeplearningWithoutH2O = true;
//                pojoPredictor = new POJOPredictor[modelHandlers.length];
//                for (int i = 0; i < modelHandlers.length; i++) {
//                    try {
//                        pojoPredictor[i] = new POJOPredictor(modelHandlers[i].getMlModel(), modelStorageLocations[i]);
//                    } catch (MLModelHandlerException e) {
//                        throw new ExecutionPlanRuntimeException(
//                                "Failed to initialize the POJO predictor of the model " + modelStorageLocations[i], e);
//                    }
//                }
//            }
//        }
//
        return Arrays.asList(new Attribute("prediction",Type.STRING));
    }

    private StructType buildSchema(List<Attribute> attributeList) {
        List<StructField> fields = new ArrayList<StructField>();
        for (Attribute attribute : attributeList) {

            StructField field = new StructField(attribute.getName(), getType(attribute.getType()), false,
                    Metadata.empty());
            fields.add(field);
        }
        return new StructType(fields.toArray(new StructField[fields.size()]));
    }

    /**
     * Convert org.wso2.siddhi.query.api.definition.Attribute.Type to org.apache.spark.sql.types.DataType
     */
    private DataType getType(Type type) {

        switch (type) {
        case STRING:
            return DataTypes.StringType;
        case DOUBLE:
            return DataTypes.DoubleType;
        case BOOL:
            return DataTypes.BooleanType;
        case FLOAT:
            return DataTypes.FloatType;
        case INT:
            return DataTypes.IntegerType;
        case LONG:
            return DataTypes.LongType;
        default:
            if (log.isDebugEnabled()) {
                log.debug("Cannot find a mapping SparkSql data type for: " + type + ". Hence, using String data type.");
            }
            return DataTypes.StringType;
        }

    }
    
    /**
     * Convert org.wso2.siddhi.query.api.definition.Attribute.Type to org.apache.spark.sql.Encoder
     */
    private Encoder<?> getEncoder(String typeString) {
        Type type = Type.valueOf(typeString.toUpperCase());
        if (type == null) {
            if (log.isDebugEnabled()) {
                log.debug("Cannot find a mapping SparkSql data type for: " + typeString + ". Hence, using String data type.");
            }
            return Encoders.STRING();
        }
        switch (type) {
        case STRING:
            return Encoders.STRING();
        case DOUBLE:
            return Encoders.DOUBLE();
        case BOOL:
            return Encoders.BOOLEAN();
        case FLOAT:
            return Encoders.FLOAT();
        case INT:
            return Encoders.INT();
        case LONG:
            return Encoders.LONG();
        default:
            if (log.isDebugEnabled()) {
                log.debug("Cannot find a mapping SparkSql data type for: " + type + ". Hence, using String data type.");
            }
            return Encoders.STRING();
        }

    }

    @Override
    public void start() {
//        try {
//            populateFeatureAttributeMapping();
//        } catch (ExecutionPlanCreationException e) {
//            log.error("Error while retrieving ML-models", e);
//            throw new ExecutionPlanCreationException("Error while retrieving ML-models" + "\n" + e.getMessage());
//        }
    }

    /**
     * Match the attribute index values of stream with feature index value of the model.
     *
     * @throws ExecutionPlanCreationException
     */
//    private void populateFeatureAttributeMapping() {
//        attributeIndexMap = new HashMap<Integer, int[]>();
//        Map<String, Integer> featureIndexMap = modelHandlers[0].getFeatures();
//        List<Integer> newToOldIndicesList = modelHandlers[0].getNewToOldIndicesList();
//
//        if (attributeSelectionAvailable) {
//            for (ExpressionExecutor expressionExecutor : attributeExpressionExecutors) {
//                if (expressionExecutor instanceof VariableExpressionExecutor) {
//                    VariableExpressionExecutor variable = (VariableExpressionExecutor) expressionExecutor;
//                    String variableName = variable.getAttribute().getName();
//                    if (featureIndexMap.get(variableName) != null) {
//                        int featureIndex = featureIndexMap.get(variableName);
//                        int newFeatureIndex = newToOldIndicesList.indexOf(featureIndex);
//                        attributeIndexMap.put(newFeatureIndex, variable.getPosition());
//                    } else {
//                        throw new ExecutionPlanCreationException(
//                                "No matching feature name found in the models for the attribute : " + variableName);
//                    }
//                }
//            }
//        } else {
//            String[] attributeNames = inputDefinition.getAttributeNameArray();
//            for (String attributeName : attributeNames) {
//                if (featureIndexMap.get(attributeName) != null) {
//                    int featureIndex = featureIndexMap.get(attributeName);
//                    int newFeatureIndex = newToOldIndicesList.indexOf(featureIndex);
//                    int[] attributeIndexArray = new int[4];
//                    attributeIndexArray[2] = 2; // get values from output data
//                    attributeIndexArray[3] = inputDefinition.getAttributePosition(attributeName);
//                    attributeIndexMap.put(newFeatureIndex, attributeIndexArray);
//                } else {
//                    throw new ExecutionPlanCreationException(
//                            "No matching feature name found in the models for the attribute : " + attributeName);
//                }
//            }
//        }
//    }
//
//    /**
//     * Return the Attribute.Type defined by the data-type argument.
//     * @param dataType data type of the output attribute
//     * @return Attribute.Type object corresponding to the dataType
//     */
//    private Attribute.Type getOutputAttributeType(String dataType) {
//
//        if (dataType.equalsIgnoreCase("double")) {
//            return Attribute.Type.DOUBLE;
//        } else if (dataType.equalsIgnoreCase("float")) {
//            return Attribute.Type.FLOAT;
//        } else if (dataType.equalsIgnoreCase("integer") || dataType.equalsIgnoreCase("int")) {
//            return Attribute.Type.INT;
//        } else if (dataType.equalsIgnoreCase("long")) {
//            return Attribute.Type.LONG;
//        } else if (dataType.equalsIgnoreCase("string")) {
//            return Attribute.Type.STRING;
//        } else if (dataType.equalsIgnoreCase("boolean") || dataType.equalsIgnoreCase("bool")) {
//            return Attribute.Type.BOOL;
//        } else {
//            throw new ExecutionPlanValidationException("Invalid data-type defined for response variable.");
//        }
//    }

//    private void logError(int modelId, Exception e) {
//        log.error("Error while retrieving ML-model : " + modelStorageLocations[modelId], e);
//        throw new ExecutionPlanCreationException(
//                "Error while retrieving ML-model : " + modelStorageLocations[modelId] + "\n" + e.getMessage());
//    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] state) {

    }
}
