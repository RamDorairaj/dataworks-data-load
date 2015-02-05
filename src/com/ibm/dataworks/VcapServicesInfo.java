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

import java.io.IOException;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;

/**
 * Wrapper for information contained in the VCAP_SERVICES environment variable.
 */
public class VcapServicesInfo
{
	//default to last known good VCAP, so when running locally, can test/debug against
	//the DataWorks service
    private String url = "";
    private String user = "";
    private String password = "";

    /**
     * Loads the VCAP services information for a specific service.
     *
     * @param serviceName the name of the service.
     */
    public VcapServicesInfo(String serviceName)
    {
    	String env = System.getenv("VCAP_SERVICES");
    	if (env == null) {
    		System.out.println("VCAP not found, using defaults");
    	} else {
            try {
                JSONObject vcapObj = JSONObject.parse(env);
                JSONArray refinerySvcs = (JSONArray)vcapObj.get(serviceName);
                JSONObject refinerySvc = (JSONObject)refinerySvcs.get(0);
                JSONObject cred = (JSONObject)refinerySvc.get("credentials");
                url = (String)cred.get("url");
                user = (String)cred.get("userid");
                password = (String)cred.get("password");
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage());
            }    		
    	}
    }
    
    /**
     * Gets the Data Works base binding URL.
     * @return the URL.
     */
    public String getDataWorksUrl()
    {
        return url;
    }
    
    /**
     * Gets the Data Works Data Load URL
     * @return
     */
    public String getDataLoadUrl()
    {
        return getDataWorksUrl() + "/dc/v1";
    }
    
    /**
     * @return the user.
     */
    public String getUser()
    {
        return user;
    }
    
    /**
     * @return the password.
     */
    public String getPassword()
    {
        return password;
    }
}
