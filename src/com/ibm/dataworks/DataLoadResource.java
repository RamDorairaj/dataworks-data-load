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
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;

import javax.net.ssl.SSLContext;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;

/**
 * This class implements a REST resource for the IBM DataWorks service.
 * 
 * It consists of one function:
 * 1. runActivity: Runs an IBM DataWorks activity.
 *                 
 *    HTTP request:                     
 *    POST to URL .../refinery
 *    Request JSON example: 
 *       { 
 *         "activityPatternId": "com.ibm.refinery.dc.DPActivityPattern",
 *         "name": "MyActivity",
 *         "inputDocument": {
 *         ...
 *         }
 *       }
 *       
 *    Response JSON example:
 *       {
 *         "activityId": "8d3905eb.529170ae.0824huvs9.9g0gah5.pr7iln.4adcqi3mp48usq5d49s4o",
 *         "activityURL": "https://xxx:9443/ibm/refinery/dc/v1/activities/8d3905eb.529170ae.0824huvs9.9g0gah5.pr7iln.4adcqi3mp48usq5d49s4o",
 *         "inputDocument" {
 *         ...
 *         },
 *         "outputDocument": {
 *         ...
 *         },
 *         "id": "8d3905eb.3d01858f.0824i2hqg.fcm9n4r.6m1upb.3igiaerh14nask40toouo",
 *         "URL": "https://vmlnxbt01:9443/ibm/refinery/dc/v1/activities/8d3905eb.529170ae.0824huvs9.9g0gah5.pr7iln.4adcqi3mp48usq5d49s4o/activityRuns/8d3905eb.3d01858f.0824i2hqg.fcm9n4r.6m1upb.3igiaerh14nask40toouo",
 *         "createdUser": "user",
 *         "createdTimeStamp": "2014-10-13T15:57:27+00:00"
 *       }
 */
@Path("/activities")
public class DataLoadResource {

	private VcapServicesInfo vcapInfo;

	/**
	 * Initializes the IBM DataWorks resource.
	 */
	public DataLoadResource() 
	{
        ResourceBundle bundle = PropertyResourceBundle.getBundle("com.ibm.dataworks.configuration");
        String serviceName = bundle.getString("dataworks.service.name");
        vcapInfo = new VcapServicesInfo(serviceName);
	}

	/**
	 * Save an activity. Return JSON containing the activity ID.
	 * 
	 * @param inputObj the input json object
	 * 
	 * @return the Response.
	 */	
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response saveActivity(JSONObject inputObj) 
	{
		try {
			//
			// Step 1: Post the activity definition .../activities
			//
		    HttpClient client = getAuthenticatedHttpClient();
			String activitiesUrl = vcapInfo.getUrl() + "/activities";
			HttpPost postRequest = new HttpPost(activitiesUrl);
			StringEntity input = new StringEntity(inputObj.serialize());
			input.setContentType(MediaType.APPLICATION_JSON);
			postRequest.setEntity(input);
			//
			// Step 2: Get the response.
			//
			HttpResponse response = client.execute(postRequest);
			int status = response.getStatusLine().getStatusCode();
			// Check the status code and return an internal server error if it is not 200
			if (status != 200) {
				JSONObject errorObject = createErrorObject("SavingActivityFailed",response);
				return Response.status(Status.INTERNAL_SERVER_ERROR).entity(errorObject).build();
			}
			//
			// Step 3: Get the URL for the activity runs resource from the output JSON
			//
			JSONObject resultObj = JSONObject.parse(response.getEntity().getContent());
            String runsUrl = (String) resultObj.get("runsURL");
			//
            // Step 4: Run the activity by sending a POST request to the URL returned by the activity creation
			//
			HttpPost runPostRequest = new HttpPost(runsUrl);
			response = client.execute(runPostRequest);
			status = response.getStatusLine().getStatusCode();
			if (status != 200) {
				JSONObject errorObject = createErrorObject("RunActivityFailed", response);
				return Response.status(Status.INTERNAL_SERVER_ERROR).entity(errorObject).build();
			}
			//
			// Step 5: return the result.
			//
			JSONObject runResponse = JSONObject.parse(response.getEntity().getContent());
			return Response.status(Status.ACCEPTED).entity(runResponse).build();
		} catch (Exception exc) {
			JSONObject errorObject = createErrorObject(exc);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(errorObject).build();
		}
	}
	
	@GET
	@Path("{activityId}/activityRuns/{runId}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getRun(	@Context                  HttpHeaders    headers, 
							@Context                  UriInfo        info,
							@PathParam("activityId")  String    	 activityId,
							@PathParam("runId")       String    	 runId) {
		try {
			//
		    // Step 1: Post the activity definition .../activities
			//
		    HttpClient client = getAuthenticatedHttpClient();
			String activityRunUrl = vcapInfo.getUrl() + "/activities/" + activityId + 
					"/activityRuns/" + runId;
			HttpGet getRequest = new HttpGet(activityRunUrl);
			getRequest.setHeader("Accept", "application/json");
			getRequest.setHeader("Content-Type", "application/json");
			
			//
			// Step 2: Get the response.
			//
			HttpResponse response = client.execute(getRequest);
			int status = response.getStatusLine().getStatusCode();
			// Check the status code and return an internal server error if it is not 200
			if (status != 200) {
				JSONObject errorObject = createErrorObject("Getting Run Status",response);
				return Response.status(Status.INTERNAL_SERVER_ERROR).entity(errorObject).build();
			}

			JSONObject getResponse = JSONObject.parse(response.getEntity().getContent());
			return Response.status(Status.ACCEPTED).entity(getResponse).build();
			
		} catch (Exception exc) {
			JSONObject errorObject = createErrorObject(exc);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(errorObject).build();
		}
	}
	
	@GET
	@Path("{activityId}/activityRuns/{runId}/logs")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getRunLogs(	@Context                  HttpHeaders    headers, 
								@Context                  UriInfo        info,
								@PathParam("activityId")  String    	activityId,
								@PathParam("runId")  String    	runId) {
		try {
			//
		    // Step 1: Post the activity definition .../activities
			//
		    HttpClient client = getAuthenticatedHttpClient();
			String activityRunLogsUrl = vcapInfo.getUrl() + "/activities/" + activityId + 
					"/activityRuns/" + runId + "/logs";
			HttpGet getRequest = new HttpGet(activityRunLogsUrl);
			getRequest.setHeader("Accept", "application/json");
			getRequest.setHeader("Content-Type", "application/json");
			
			//
			// Step 2: Get the response.
			//
			HttpResponse response = client.execute(getRequest);
			int status = response.getStatusLine().getStatusCode();
			// Check the status code and return an internal server error if it is not 200
			if (status != 200) {
				JSONObject errorObject = createErrorObject("Getting Run Logs Failed",response);
				return Response.status(Status.INTERNAL_SERVER_ERROR).entity(errorObject).build();
			}

			JSONArray getResponse = JSONArray.parse(response.getEntity().getContent());
			return Response.status(Status.ACCEPTED).entity(getResponse).build();
			
		} catch (Exception exc) {
			JSONObject errorObject = createErrorObject(exc);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(errorObject).build();
		}
	}
	
	/**
	 * Create a JSON object containing a simple error message and additional details.
	 */
	private JSONObject createErrorObject(String errorMessage, HttpResponse response) 
	{
		String msgId = "";
		String msgSeverity = "error";
		String msgText = errorMessage;
		String msgExplanation = "";
		String msgResponse = "";
		
		try {
			if (response.getEntity() != null) {
				InputStream is = response.getEntity().getContent();
				if (is != null) {
					JSONObject errObj = JSONObject.parse(is);
					msgId = (String)errObj.get("msgId");
					msgSeverity = (String)errObj.get("msgSeverity");
					msgText = (String)errObj.get("msgText");
					msgExplanation = (String)errObj.get("msgExplanation");
					msgResponse = (String)errObj.get("msgResponse");
				}
			}
		} catch (IllegalStateException e) {
			return createErrorObject(e);
		} catch (IOException e) {
			return createErrorObject(e);
		}
		
		return createErrorObject(msgId, msgSeverity,msgText,
				msgExplanation, msgResponse);
	}

	/**
	 * Create an error JSON object from an exception.
	 */
	private JSONObject createErrorObject(Exception exc) 
	{
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		exc.printStackTrace(pw);
		pw.close();
		String details = sw.toString();
		return this.createErrorObject("500", "error", details, "", "");
	}
	
	/**
	 * Create an appropriate response payload.
	 */
	private JSONObject createErrorObject(String msgId, String msgSeverity, 
			String msgText, String msgExplanation, String msgResponse) {
		JSONObject json = new JSONObject();
		json.put("msgId", msgId);
		json.put("msgSeverity", msgSeverity);
		json.put("msgText", msgText);
		json.put("msgExplanation", msgExplanation);
		json.put("msgResponse", msgResponse);
		
		return json;
	}


	/** 
	 * Create an HTTP client object that is authenticated with the user and password
	 * of the IBM DataWorks Service.
	 */
	private HttpClient getAuthenticatedHttpClient() throws GeneralSecurityException {
		// NOTE: If you re-purpose this code for your own application you might want to have 
		// additional security mechanisms in place regarding certificate authentication.

		// build credentials object
		UsernamePasswordCredentials creds = new UsernamePasswordCredentials(vcapInfo.getUser(), vcapInfo.getPassword());
		CredentialsProvider credsProvider = new BasicCredentialsProvider();
		credsProvider.setCredentials(new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT), creds);
		// always accept the certificate 
		TrustStrategy accepAllTrustStrategy = new TrustStrategy() {
			@Override
			public boolean isTrusted(X509Certificate[] certificate, String authType) {
				return true;
			}
		};
		SSLContextBuilder contextBuilder = new SSLContextBuilder();
		SSLContext context = contextBuilder.loadTrustMaterial(null, accepAllTrustStrategy).build();
		SSLConnectionSocketFactory scsf = new SSLConnectionSocketFactory(context, new AllowAllHostnameVerifier());

		HttpClient httpClient = HttpClientBuilder.create() //
				.setSSLSocketFactory(scsf) //
				.setDefaultCredentialsProvider(credsProvider) //
				.build();

		return httpClient;
	}
	
	
}
