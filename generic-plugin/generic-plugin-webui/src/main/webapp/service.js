'use strict';

angular.module('subutai.plugins.generic.service',[])
	.factory('genericSrv', genericSrv);

genericSrv.$inject = ['$http', 'environmentService'];


function genericSrv($http, environmentService) {
	var BASE_URL = SERVER_URL + 'rest/generic/';

	var genericSrv = {
		getPluginInfo: getPluginInfo,
		listProfiles: listProfiles,
		saveProfile: saveProfile,
		deleteProfile: deleteProfile,

		listOperations: listOperations,
		saveOperation: saveOperation,
		updateOperation: updateOperation,
		deleteOperation: deleteOperation,

		getEnvironments: getEnvironments,
		executeOperation: executeOperation
	};

	return genericSrv;

	// Create

	function listProfiles() {
		return $http.get (BASE_URL + "profiles");
	}

	function saveProfile (profile) {
		var postData = "profileName=" + profile;
		return $http.post (BASE_URL + "profiles/create", postData, {withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}});
	}


	function deleteProfile (profile) {
		return $http.delete (BASE_URL + "profiles/" + profile.id);
	}


	// Manage

	function listOperations (profile) {
		return $http.get (BASE_URL + "operations/" + profile.name);
	}


	function saveOperation (profile, operation) {
		var fd = new FormData();
		fd.append('profileName', profile.name);
		fd.append('operationName', operation.operationName);
		fd.append('file', operation.commandName);
		fd.append('cwd', operation.cwd);
		fd.append('timeout', operation.timeout);
		fd.append('daemon', operation.daemon);
		fd.append('script', operation.script);
		return $http.post(
			BASE_URL + 'operations/script/create',
			fd,
			{transformRequest: angular.identity, headers: {'Content-Type': undefined}}
		);
	}

	function updateOperation (operation, file) {
		var fd = new FormData();
		fd.append('operationId', operation.operationId);
		fd.append('file', operation.commandName);
		fd.append('cwd', operation.cwd);
		fd.append('timeout', operation.timeout);
		fd.append('daemon', operation.daemon);
		fd.append('script', operation.script);
		fd.append('operationName', operation.operationName);
		return $http.post(
			BASE_URL + 'operations/script/update',
			fd,
			{transformRequest: angular.identity, headers: {'Content-Type': undefined}}
		);
	}

	function deleteOperation (operationId) {
		return $http.delete (BASE_URL + "operations/" + operationId);
	}

	function getEnvironments() {
		return environmentService.getEnvironments();
	}


	function executeOperation (operationId, lxcHostName, environmentId) {
		var putData = "operationId=" + operationId + "&lxcHostName=" + lxcHostName + "&environmentId=" + environmentId;
		console.log (putData);
		return $http.put (BASE_URL + "execute", putData, {withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}});
	}

	function getPluginInfo() {
    	return $http.get (BASE_URL + "about", {withCredentials: true, headers: {'Content-Type': 'application/json'}});
    }
}
