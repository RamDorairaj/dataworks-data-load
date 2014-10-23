/*	
 * Copyright IBM Corp. 2014
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.dataworks;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.Application;

/**
 * Sample application showing the usage of the data refinery REST API.
 */
public class DataLoadApplication extends Application 
{
    /**
     * {@inheritDoc}
     */
	@Override
	public Set<Class<?>> getClasses() 
	{
		Set<Class<?>> classes = new HashSet<Class<?>>();
		classes.add(DataLoadResource.class);
		return classes;
	}
}
