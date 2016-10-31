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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.io.File;
import java.net.URISyntaxException;

public class PredictStreamProcessorTestCase {

    private volatile boolean eventArrived;
    private String modelStorageLocation = System.getProperty("user.dir") + File.separator + "src" + File.separator
            + "test" + File.separator + "resources" + File.separator + "model";

    @Before
    public void init() {
        eventArrived = false;
    }

    @Test
    public void predictFunctionTest() throws InterruptedException, URISyntaxException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String inputStream = "define stream InputStream "
                + "(id long, text string);";

        String query = "@info(name = 'query1') " + "from InputStream#ml:predict('" + modelStorageLocation
                + "') " + "select * " + "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inputStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    Assert.assertEquals("0.0", inEvents[0].getData(2));
                    eventArrived = true;
                }
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[] {4L, new String("spark i j k")});
        sleepTillArrive(20000);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test
    public void predictFunctionWithSelectedAttributesTest() throws InterruptedException, URISyntaxException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String inputStream = "define stream InputStream "
                + "(id long, text string);";

        String query = "@info(name = 'query1') " + "from InputStream#ml:predict('" + modelStorageLocation
                + "','double') " + "select * "
                + "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inputStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    Assert.assertEquals(0.0, inEvents[0].getData(2));
                    eventArrived = true;
                }
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[] {4L, new String("spark i j k")});
        sleepTillArrive(20000);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void predictFunctionWithSelectPredictionTest() throws InterruptedException, URISyntaxException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String inputStream = "define stream InputStream " + "(id long, text string);";

        String query = "@info(name = 'query1') " + "from InputStream#ml:predict() " + "select * "
                + "insert into outputStream ;";

        try {
            siddhiManager.createExecutionPlanRuntime(inputStream + query);
        } catch (Exception e) {
            Assert.assertEquals(e instanceof ExecutionPlanValidationException, true);
        }

    }

    private void sleepTillArrive(int milliseconds) {
        int totalTime = 0;
        while (!eventArrived && totalTime < milliseconds) {
            int t = 1000;
            try {
                Thread.sleep(t);
            } catch (InterruptedException ignore) {
            }
            totalTime += t;
        }
    }
}
