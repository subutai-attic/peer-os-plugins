"use strict";
angular.module ("subutai.plugins.backup.controller", [])
	.controller("BackupCtrl", BackupCtrl);

BackupCtrl.$inject = ["$scope", "backupSrv", "SweetAlert", "ngDialog"];

function BackupCtrl ($scope, backupSrv, SweetAlert, ngDialog) {
	var vm = this;

	vm.environments = [];
	vm.currentEnvironment = {};
	vm.currentContainer = {};

	backupSrv.getEnvironments().success (function (data) {
		console.log (data);
		vm.environments = data;
		vm.currentEnvironment = vm.environments[0];
	});

	vm.backupWindow = backupWindow;
	function backupWindow (container) {
		vm.currentContainer = container;
		ngDialog.open ({
			template: "plugins/backup/partials/backup.html",
			scope: $scope
		});
	}


	vm.backup = backup;
	function backup (container) {
		backupSrv.backup (container.id).success (function (data) {
			SweetAlert.swal ("Success!", "Container is scheduled for backup.", "success");
		}).error (function (error) {
			SweetAlert.swal ("ERROR!", "Container backup error: " + error.replace(/\\n/g, " "), "error");
		});
	}
}