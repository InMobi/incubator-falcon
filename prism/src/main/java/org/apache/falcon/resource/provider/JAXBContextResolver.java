/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.resource.provider;

import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;
import org.apache.falcon.resource.InstancesSummaryResult.InstanceSummary;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

/**
 * An implementation of Context Resolver for JAXB.
 */
@Provider
@Produces(MediaType.APPLICATION_JSON)
public class JAXBContextResolver implements ContextResolver<JAXBContext> {

    private static JAXBContext context;
    private static Class<?>[] types = {
        InstancesResult.class,
        APIResult.class,
        InstancesResult.Instance.class,
        InstancesResult.WorkflowStatus.class,
        InstancesSummaryResult.class,
        InstanceSummary.class,
    };

    static {
        try {
            context = new JSONJAXBContext(JSONConfiguration.natural().build(), types);
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }
    }

    public JAXBContextResolver() {
    }

    public JAXBContext getContext(Class<?> objectType) {
        for (Class<?> type : types) {
            if (type == objectType) {
                return context;
            }
        }
        return null;
    }
}
