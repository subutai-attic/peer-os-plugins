"use strict";

angular.module ("subutai.plugins.backup.service",[])
	.factory ("backupSrv", backupSrv);

backupSrv.$inject = ["$http", "environmentService"];


function backupSrv ($http, environmentService) {
	var backupSrv = {
		getEnvironments: getEnvironments,
		backup: backup
	};


	function getEnvironments() {
		return environmentService.getEnvironments();
	}

	function backup() {
	}

	return backupSrv;
}