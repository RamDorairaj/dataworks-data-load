/*
 *  Copyright IBM Corp. 2014
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

/** track the current activity and run ids, for subsequent View Logs calls. */
var currentActivityId;
var currentRunId;
var pollingTimer;

/**
 * createAndRunActivity:
 * This code takes the user input and prepares the payload for the activity request. 
 * Then, the servlet creates an activity by calling the POST /activities method on the IBM DataWorks API. 
 * Immediately after the activity is created, the servlet runs the activity by calling the 
 * POST /activities/<activityId>/activityRuns method. 
 * The servlet responds to the client with the payload response.
 */
function createAndRunActivity() {
	
	// disable the Load Data and the View Log buttons
	$('#createBtn').prop('disabled', true);
	$('#logBtn').prop('disabled', true);

	// hide success and failure messages and empty result table
	$("#successContainer").hide();
	$("#failureContainer").hide();
	$("#resultContainer").hide();
	$('#resultText').val('');
	
	// display the progress indicator
	$("#loadingImg").show();
	
	// Hold UI parameters used by sources and targets
	var srcTable;	
	var targetTable;	
	var uniqueId = new Date().getTime();
	var sourceConnection;
	var srcConnType = $("#sourceSystemSelect").val();
	var targetConnection;
	var trgConnType = $("#targetSystemSelect").val();
	var srcOptions;
	var trgOptions;
	var srcTableDefinition;
		

	// ============  SOURCE =====================
	// Set up the request when the source is an on-premises Oracle database.
	if (srcConnType == "ORACLEODBC") {
		var srcHost     = $("#srcORACHostInput").val();
		var srcPort     = $("#srcORACPortInput").val();
		var srcSchema   = $("#srcORACSchemaInput").val();
		var srcUsername = $("#srcORACUsernameInput").val();
		var srcPassword = $("#srcORACPasswordInput").val();
		var srcSid		= $("#srcORACSIDInput").val();
		var srcSSLCert	= $("#srcORACSSLCert").val();		
		srcTable    	= $("#srcORACTableInput").val();	
		var isSSL = false;
		
		if(srcSSLCert && srcSSLCert.trim())
			isSSL = true;
		
		sourceConnection =  {
	            type : "oracle",
				sid : srcSid,
	            user : srcUsername,
	            password : srcPassword,
	            schema : srcSchema,
	            host : srcHost,
	            port : parseInt(srcPort),
	            ssl : isSSL,
	            sslCert : srcSSLCert
	        };
		
        srcOptions = {
            maxRecordPerTable : 1000000,
            maxTableToExtract : "all"
        };
		
	// setup the request for an HADOOP source		
	} else if (srcConnType == "HADOOP") {
		
		var srcHadoopFolderPath = $("#srcHadoopFolderPathInput").val();
		var srcHadoopHost     = $("#srcHadoopHostInput").val();
		var srcHadoopPort     = $("#srcHadoopPortInput").val();
		var srcHadoopUsername = $("#srcHadoopUsernameInput").val();
		var srcHadoopPassword = $("#srcHadoopPasswordInput").val();
		srcTable    		  = $("#srcHadoopTableInput").val();
		
		sourceConnection = {
                type : "hadoop",
                httpFs : {
                    host : srcHadoopHost,
                    port : parseInt(srcHadoopPort),
                    user : srcHadoopUsername,
                    password : srcHadoopPassword,
                    folderPath : srcHadoopFolderPath
                	}
                };
		
		srcOptions = {
		            structureInFirstRow : false,
			        fileStructure : "csv",
			        encoding : "UTF-8",
			        maxRecordPerFile : "all"
				};
		
	// setup the request for the SoftLayer Object Storage (Swift) source		
	} else if (srcConnType == "OBJECTSTORAGE") {
		
		var srcObjURLInput 		 = $("#srcObjURLInput").val();
		var srcObjContainerInput = $("#srcObjContainerInput").val();
		var srcObjAccessKeyInput = $("#srcObjAccessKeyInput").val();
		var srcObjSecretKeyInput = $("#srcObjSecretKeyInput").val();
		srcTable    			 = $("#srcObjTableInput").val();
		
		sourceConnection = {
				type :"softlayerobjectstorage",
                loginUrl : srcObjURLInput,
                container : srcObjContainerInput,
                accessKey : srcObjAccessKeyInput,
                secretKey : srcObjSecretKeyInput
                };
		
		srcOptions = {
		            structureInFirstRow : false,
			        fileStructure : "csv",
			        encoding : "UTF-8",
			        maxRecordPerFile : "all"
				};		

	// setup the request for the Amazon S3 source (csv)		
	} else if (srcConnType == "AMAZONS3") {
		
		var srcS3RegionInput 	= $("#srcS3RegionInput").val();
		var srcS3BucketInput 	= $("#srcS3BucketInput").val();	
		var srcS3AccessKeyInput = $("#srcS3AccessKeyInput").val();
		var srcS3SecretKeyInput = $("#srcS3SecretKeyInput").val();
		srcTable    			= $("#srcS3TableInput").val();		
	
		sourceConnection = {
                type :"amazons3",
                region : srcS3RegionInput,
                bucket : srcS3BucketInput,
                accessKey : srcS3AccessKeyInput,
                secretKey : srcS3SecretKeyInput
                };
		
		srcOptions = {
		            structureInFirstRow : false,
			        fileStructure : "csv",
			        encoding : "UTF-8",
			        maxRecordPerFile : "all"
				};	

	// setup the request for a DashDB or SQLDB source
	} else {	
		
		var srcHost     = $("#srcHostInput").val();
		var srcPort     = $("#srcPortInput").val();
		var srcDatabase = $("#srcDatabaseInput").val();
		var srcSchema   = $("#srcSchemaInput").val();
		var srcUsername = $("#srcUsernameInput").val();
		var srcPassword = $("#srcPasswordInput").val();
		srcTable    	= $("#srcTableInput").val();	
		var srcType     = "dashdb";
		
		if (srcConnType == "SQLDB") {
			srcType     = "sqldb";
		}
		
		sourceConnection =  {
           	database : srcDatabase,
           	user : srcUsername,
           	password : srcPassword,
           	schema : srcSchema,
           	host: srcHost,
           	port: parseInt(srcPort),
           	type : srcType
        };
		
        srcOptions = {
                maxRecordPerTable : 1000000,
                maxTableToExtract : "all"
            };
	}
	
	// When source is HADOOP, AMAZONS3 or OBJECTSTORAGE, the csv file columns need to be defined in the JSON.
	// this sample code only supports a CSV file that contains 2 columns:  integer, varchar. 
	// Input csv file has to be in format:
	// 		10,"a"
	// 		20,"bb"
	//  	30,"cccc"
	if (srcConnType == "HADOOP" || srcConnType == "AMAZONS3" || srcConnType == "OBJECTSTORAGE") {
		 srcTableDefinition= { 
			        		columns : [
					          		  {  name: "ID",
					          		     type :	{
					          		           	length: 10,
					          		           	type: "integer"
					          			    	}
					          		  },
					          		  {  name: "NAME",
					          		     type: 	{
					          		           	length: 20,
					          		           	type: "varchar"
					          		     		}
					          		  }
					          		  ],
			                id: "s1",
			                name: srcTable
						};	
	} else {
		// other sources do not need the table-columns definition
		srcTableDefinition = 	{
									id : "s1",
									name : srcTable
								};		
	}
	
	
	// ============  TARGET =====================
	// setup the request when the target is Analytics for Hadoop
	if (trgConnType == "HADOOP") {
		
		var targetHadoopFolderPath = $("#targetHadoopFolderPathInput").val();
		var targetHadoopHost     = $("#targetHadoopHostInput").val();
		var targetHadoopPort     = $("#targetHadoopPortInput").val();
		var targetHadoopUsername = $("#targetHadoopUsernameInput").val();
		var targetHadoopPassword = $("#targetHadoopPasswordInput").val();
		targetTable    			 = $("#targetHadoopTableInput").val();
		
		targetConnection = {
            type : "hadoop",
            httpFs : {
                    host : targetHadoopHost,
                    port : parseInt(targetHadoopPort),
                    user : targetHadoopUsername,
                    password : targetHadoopPassword,
                    folderPath : targetHadoopFolderPath
                }
        };
		
		trgOptions = {
            encoding : "UTF-8"
        };
		

	// setup the request for targets DashDB or SQLDB
	} else {
		var targetHost     	= $("#targetHostInput").val();
		var targetPort     	= $("#targetPortInput").val();
		var targetDatabase 	= $("#targetDatabaseInput").val();
		var targetSchema   	= $("#targetSchemaInput").val();
		var targetUsername 	= $("#targetUsernameInput").val();
		var targetPassword 	= $("#targetPasswordInput").val();
		targetTable			= $("#targetTableInput").val();
		var targetType      = "dashdb";
		
		if (trgConnType == "SQLDB") {
			targetType = "sqldb";
		}
		
		
		targetConnection = 	{
	            database : targetDatabase,
	            user : targetUsername,
	            password : targetPassword,
	            schema : targetSchema,
	            host : targetHost,
	            port: parseInt(targetPort),
	            type : targetType
	        };
		
		trgOptions = {
				existingTablesAction : "append"		        	
        	};
	}
	

	
	// REQUEST BODY
	// define request body
	body = {
	    activityPatternId : "DataLoad",
	    name : "SDL_" + uniqueId,
	    inputDocument : {
	        name : "SDL_" + uniqueId,
	        sourceOptions : srcOptions,
	        targetOptions : trgOptions,
	        
	        target : {
	            connection : targetConnection,

	            tables : [{
	                    name : targetTable,
	                    sourceIds : ["s1"]
	                }
	            ]
	        },
	        sources : 	[{
		                  connection : sourceConnection,

		                  tables : [ 
		                             srcTableDefinition	
		                           ]
	            		}]
	    	},
	    shortDescription : "A sample activity"
	};
	
	//create the activity and then run it
	createActivity(body).then(function(activity){
		runActivity(activity);
	});
};

/**
 * createActivity: 
 * Create the activity.  
 * The servlet creates an activity by calling the POST /activities method on the IBM DataWorks API. 
 * Returns the response from the POST call or displays the error message, if an error occurred.
 * @param {string} activityId
 */
function createActivity(activity) {
	return $.ajax({
		url : "refinery/activities", 
		type : "POST",
		data : JSON.stringify(body),
		headers : {
			'Content-Type' : 'application/json'
		},
		dataType : "json"
	}).then(
		function(data) {
			// track the current activity info
			currentActivityId = data.id;
			
			$('#successMsg').html('The activity was created.');
			$("#successContainer").show();
			
			return data;
		},
		function(data) {
			$('#createBtn').prop('disabled', false);
			$('#logBtn').prop('disabled', true);
			
			$("#loadingImg").hide();
			displayErrorMessage("Error creating the activity:<br/>" + data.responseText);
		});
};

/**
 * runActivity: 
 * The servlet runs the activity by calling the POST /activities/<activityId>/activityRuns method. 
 * The servlet responds to the client with the payload response.
 * It then starts a results-polling function, or displays the error message, if an error occurred.
 */
function runActivity(activity) {
	return $.ajax({
		url : "refinery/activities/" + activity.id + "/activityRuns", 
		type : "POST",
		headers : {
			'Accept' : 'application/json'
		},
		dataType : "json"
	}).then(
		function(data) {
			// track the current run info
			currentRunId = data.id;
			$('#successMsg').html('The run was started.');
			$("#successContainer").show();
			$('#logBtn').prop('disabled', false);
			
			// poll for results every 30 seconds
			pollForResults(data.activityId, data.id, 30000);
		},
		function(data) {
			$('#createBtn').prop('disabled', false);
			$('#logBtn').prop('disabled', true);
			
			$("#loadingImg").hide();
			displayErrorMessage("Error running the activity:<br/>" + data.responseText);
			
			return data;
		});
}

/**
 * pollForResults: 
 * This function starts a poll for activity-run results at regular time intervals.
 * @param {string} activityId
 * @param {string} runId
 * @param {number} pollingInterval
 */
function pollForResults(activityId, runId, pollingInterval) {
	//clear any existing
	if (pollingTimer) {
		clearInterval(pollingTimer);
		pollingTimer = null;
	}
	
	// start timer loop for polling every <pollingInterval> milliseconds
	pollingTimer = setInterval(function () {
		// get status from the IBM DataWorks service
		getRunStatus(
			activityId,
			runId,
			function(){	
				// stop timer loop
				clearInterval(pollingTimer);
			}
		);
	}, pollingInterval);		
};

/**
 * reset: 
 * This function resets the form and other states information to the initial state. The form is 
 * not cleared of its data, so the user can re-use the same input data for additional runs. 
 */
function reset() {
	//clear any existing status polling timer
	if (pollingTimer) {
		clearInterval(pollingTimer);
		pollingTimer = null;
	}
	
	$("#loadingImg").hide();
	$("#successContainer").hide();
	$("#failureContainer").hide();
	$("#resultContainer").hide();
	$('#InfoContainer').hide(); 
	$('#infoMsgContainer').hide();
	$('#resultText').val("");
	
	$('#createBtn').prop('disabled', false);
	$('#logBtn').prop('disabled', true);
	
	currentActivityId = null;
	currentRunId = null;
}

/**
 * getRunStatus: 
 * This code calls a REST service to get the status of the activity run 
 * by invoking the IBM DataWorks activities/<activityId>/activityRuns/<runId> method,
 * and responds to the UI with the latest ActivityRun response payload, which includes status information.
 * @param {string} activityId
 * @param {string} runId
 * @param {function} stopPollingCallback
 */
function getRunStatus (activityId, runId, stopPollingCallback) {
	$.ajax({
		url : "refinery/activities/" + activityId + "/activityRuns/" + runId,
		headers : {
			'Accept' : 'application/json',
			'Content-Type' : 'application/json'
		},
		success : function(data) { 
			populateResults(data, activityId, runId);
			if  (data.outputDocument.common.status.indexOf("FINISHED") != -1) {
				$("#loadingImg").hide();
				stopPollingCallback();
			};
		},
		error : function(data) {
			$("#loadingImg").hide();
			displayErrorMessage("The status of the activity run was not retrieved:<br/>" + JSON.stringify(data, null, true)); 
			stopPollingCallback();
		}
	});
};

/**
 * populateResults:
 * This function populates the result panel on the website with either success results or error diagnostic information
 * @param {object} runResult
 * @param {string} activityId
 * @param {string} runId
 */
function populateResults(runResult, activityId, runId) {
	// Remove other panels
	$('#failureContainer').hide();
	$("#successContainer").hide();
	$('#InfoContainer').hide(); 
	$('#infoMsgContainer').hide();

	if (runResult.outputDocument.common.status.indexOf("FINISHED") != -1) {
		if (runResult.outputDocument.common.status.indexOf("ERROR") == -1) {
			// display success message
			$('#successMsg').html('The data was loaded successfully. ' + runResult.outputDocument.rowsMoved + ' records were processed. For more information, click View Log.');
			$("#successContainer").show();
		} else {
			// display error message with diagnostic information
			$('#errorMsg').html('The data was not loaded because the following error occurred:<br/>' +
								' <br/>' +
					            '<b>Execution ID:</b> ' + runResult.outputDocument.common.executionId +  '<br/>' +
					            '<b>runBy:</b> ' + runResult.outputDocument.common.runBy +  '<br/>' +
								' <br/>' +
					            '<b>ActivityID:</b> ' + activityId +  '<br/>' +
					            '<b>RunID:</b> ' + runId +  '<br/>' +
								' <br/>' +
					            '<b>Status:</b> ' + runResult.outputDocument.common.status + '<br/>' +
					            'For more information, click View Log.');	
			$("#failureContainer").show();
		}
	}
};

/**
 * displayErrorMessage:
 * This function displays the provided error message on the website.
 * @param {string} message to display
 */
function displayErrorMessage(message) {
	// hide progress indicator and other results containers
	$("#loadingImg").hide();
	$("#resultContainer").hide();
	$("#successContainer").hide();
	$('#InfoContainer').hide(); 
	$('#infoMsgContainer').hide();
	
	// display the error message
	if (message) {
		$('#errorMsg').html(message);
	} else {
		$('#errorMsg').html('The data loading process was not completed.');			
	}
	$("#failureContainer").show();				
};


/**
 * viewLog:
 * This code calls a REST service to get the detailed log text of the activity run 
 * by invoking the IBM DataWorks activities/<activityId>/activityRuns/<runId>/logs method.
 * If successful, it provides the response payload in the results panel, or displays the error returned. 
 * @param {string} activityId
 * @param {string} runId
 */
function viewLog(activityId, runId){

	$('#resultText').val('Loading...');
	$("#successContainer").hide();
	$("#failureContainer").hide();
	$('#InfoContainer').hide(); 
	$('#infoMsgContainer').hide();
	$("#resultContainer").show();
	$.ajax({
		url : "refinery/activities/" + (activityId || currentActivityId) + 
			"/activityRuns/" + (runId || currentRunId) + "/logs",
		headers : {
			'Accept' : 'application/json',
			'Content-Type' : 'application/json'
		},
		success : function(data) { 
			$("#resultText").val(JSON.stringify(data, null, " "));
		},
		error : function(data) {
			displayErrorMessage("The log for the activity run could not be retrieved:<br/>" + JSON.stringify(data, null, true)); 
		}
	});
	
};

/**
 * viewPayload:
 * This function removes from view any results panels and displays instructions on how to view the payload information
 */
function viewPayload(){

	$("#successContainer").hide();
	$("#failureContainer").hide();	
	$("#resultContainer").hide();	
	$('#infoMsgContainer').hide();
	$('#InfoContainer').show(); 

};
