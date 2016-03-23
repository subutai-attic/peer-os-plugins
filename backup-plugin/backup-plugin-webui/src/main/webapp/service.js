"use strict";

angular.module ("subutai.plugins.backup.service",[])
	.factory ("backupSrv", backupSrv);

backupSrv.$inject = ["$http", "environmentService"];


function backupSrv ($http, environmentService) {

	var BASE_URL = SERVER_URL + "rest/backup/";

	var backupSrv = {
		getEnvironments: getEnvironments,
		backup: backup
	};


	function getEnvironments() {
		return environmentService.getEnvironments();
	}

	function backup (container) {
		var postData = 'lxcHostName=' + container.hostname;
		console.log (postData);
		return $http.post(
			BASE_URL + "container",
			postData,
			{withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
		);
	}

	return backupSrv;
}