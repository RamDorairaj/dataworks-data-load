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
 * Create and run the activity.  
 * 
 * Takes the user inputs and invokes the servlet with an activity request payload.
 * The servlet will then create an activity by calling POST /activities on the 
 * IBM DataWorks api, and the servlet will then immediately run the activity
 * by calling /activities/<activityId>/activityRuns. The servlet will then respond
 * back to the client with the ActivityRun paylod response.
 */
function createAndRunActivity() {
	$('#createBtn').prop('disabled', true);
	$('#logBtn').prop('disabled', true);
	
	// retrieve parameters from html input fields
	var srcHost     = $("#srcHostInput").val();
	var srcPort     = $("#srcPortInput").val();
	var srcDatabase = $("#srcDatabaseInput").val();
	var srcSchema   = $("#srcSchemaInput").val();
	var srcUsername = $("#srcUsernameInput").val();
	var srcPassword = $("#srcPasswordInput").val();
	var srcTable    = $("#srcTableInput").val();
	
	var targetHost     = $("#targetHostInput").val();
	var targetPort     = $("#targetPortInput").val();
	var targetDatabase = $("#targetDatabaseInput").val();
	var targetSchema   = $("#targetSchemaInput").val();
	var targetUsername = $("#targetUsernameInput").val();
	var targetPassword = $("#targetPasswordInput").val();
	var targetTable    = $("#targetTableInput").val();
	
	// hide success and failure messages and empty result table
	$("#successContainer").hide();
	$("#failureContainer").hide();
	$("#resultContainer").hide();
	$('#resultText').val('');
	
	// display progress indicator
	$("#loadingImg").show();

	var uniqueId = new Date().getTime();
	// define request body
	body = {
		    "activityPatternId" : "com.ibm.refinery.dc.DPActivityPattern",
		    "name": "SDL_" + uniqueId,
		    "inputDocument" : {
		        "name": "SDL_" + uniqueId,
		        "sourceOptions" : {
		            "maxRecordPerTable" : 1000000,
		            "maxTableToExtract" : "all"
		        },
		        "targetOptions" : {
		            "appendToExistingTables" : true,
		            "replaceExistingTables" : false
		        },
		        "target" : {
		            "connection" : {
		                "database" : targetDatabase,
		                "userName" : targetUsername,
		                "password" : targetPassword,
		                "schema" : targetSchema,
		                "hostName": targetHost,
		                "port": parseInt(targetPort),
		                "type" : "db2"
		            },
		            
		            "tables" : [{
		                    "name" : targetTable,
		                    "tableAlreadyExists" : false,
		                    "sourceIds" : ["s1"]
		                }
		            ]
		        },
		        "sources" : [{
		                "connection" : {
		                	"database" : srcDatabase,
			                "userName" : srcUsername,
			                "password" : srcPassword,
			                "schema" : srcSchema,
			                "hostName": srcHost,
			                "port": parseInt(srcPort),
			                "type" : "db2"
		                },
		                
		                "tables" : [{
		                        "id" : "s1",
		                        "name" : srcTable
		                    }
		                ]
		            }
		        ]
		    },
		    "shortDescription" : "A sample activity"
		};
	
	// start data loading process
	$.ajax({
		"url" : "refinery/activities", 
		"type" : "POST",
		"data" : JSON.stringify(body),
		"headers" : {
			'Content-Type' : 'application/json'
		},
		"dataType" : "json",
		"success" : function(data) {
			//track the current activity and run info
			currentActivityId = data.activityId;
			currentRunId = data.id;
			
			$('#successMsg').html('The activity was created and the activity run started.');
			$("#successContainer").show();
			$('#logBtn').prop('disabled', false);
			
			// poll for results every 30 seconds
			pollForResults(data.activityId, data.id, 30000);
		},
		"error" : function(data) {
			$('#createBtn').prop('disabled', false);
			$('#logBtn').prop('disabled', true);
			
			$("#loadingImg").hide();
			displayErrorMessage("Error creating and running the activity:<br/>" + JSON.stringify(data, null, true));  
		} 
	});
};

/**
 * Poll for activity run results in regular time intervals
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
 * Reset the form and other state
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
	$('#resultText').val("");
	
	$('#createBtn').prop('disabled', false);
	$('#logBtn').prop('disabled', true);
	
	currentActivityId = null;
	currentRunId = null;
}

/**
 * Call REST service to get most current run status. The servlet will then invoke
 * the IBM DataWorks activities/<activityId>/activityRuns/<runId> api and respond
 * back to the UI with the latest ActivityRun response payload, which includes
 * status information.
 * @param {string} activityId
 * @param {string} runId
 * @param {function} stopPollingCallback
 */
function getRunStatus (activityId, runId, stopPollingCallback) {
	$.ajax({
		"url" : "refinery/activities/" + activityId + "/activityRuns/" + runId,
		"headers" : {
			'Accept' : 'application/json',
			'Content-Type' : 'application/json'
		},
		"success" : function(data) { 
			populateResults(data);
			if  (data.outputDocument.common.status.indexOf("FINISHED") != -1) {
				$("#loadingImg").hide();
				stopPollingCallback();
			};
		},
		"error" : function(data) {
			$("#loadingImg").hide();
			displayErrorMessage("The status of the activity run was not retrieved:<br/>" + JSON.stringify(data, null, true)); 
			stopPollingCallback();
		}
	});
};

/**
 * Populate result table on website
 * @param {object} classificationResult
 */
function populateResults(runResult) {
	// empty result table
	$('#failureContainer').hide();
	$("#successContainer").hide();

	if (runResult.outputDocument.common.status.indexOf("FINISHED") != -1) {
		if (runResult.outputDocument.common.status.indexOf("ERROR") == -1) {
			// display success message
			$('#successMsg').html('The data was loaded successfully. ' + runResult.outputDocument.rowsMoved + ' records were processed. For more information, click View Log.');
			$("#successContainer").show();
		} else {
			// display success message
			$('#errorMsg').html('The data was not loaded because the following error occurred: ' + runResult.outputDocument.common.status + '. For more information, click View Log.');	
			$("#failureContainer").show();
		}
	}
};

/**
 * Display error message on website
 * @param {string} message
 */
function displayErrorMessage(message) {
	// hide progress indicator
	$("#loadingImg").hide();
	// display error message
	if (message) {
		$('#errorMsg').html(message);
	} else {
		$('#errorMsg').html('The data loading process was not completed.');			
	}
	$("#resultContainer").hide();
	$("#successConainer").hide();
	$("#failureContainer").show();				
};


/**
 * Display detailed log text
 */
function viewLog(activityId, runId){

	$('#resultText').val('Loading...');
	$("#successContainer").hide();
	$("#failureContainer").hide();	
	$("#resultContainer").show();
	$.ajax({
		"url" : "refinery/activities/" + (activityId || currentActivityId) + 
			"/activityRuns/" + (runId || currentRunId) + "/logs",
		"headers" : {
			'Accept' : 'application/json',
			'Content-Type' : 'application/json'
		},
		"success" : function(data) { 
			$("#resultText").val(JSON.stringify(data, null, " "));
		},
		"error" : function(data) {
			displayErrorMessage("The log for the activity run could not be retrieved:<br/>" + JSON.stringify(data, null, true)); 
		}
	});
	
};