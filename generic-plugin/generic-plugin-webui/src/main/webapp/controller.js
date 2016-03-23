"use strict";
angular.module("subutai.plugins.generic.controller", [])
    .controller("GenericCtrl", GenericCtrl)
    .config(['terminalConfigurationProvider', function (terminalConfigurationProvider) {
		terminalConfigurationProvider.config('modern').allowTypingWriteDisplaying = false;
		terminalConfigurationProvider.config('modern').outputDelay = 0;
	}]);

GenericCtrl.$inject = ["$scope", "genericSrv", "SweetAlert", "DTOptionsBuilder", "DTColumnDefBuilder", "ngDialog"];

function GenericCtrl($scope, genericSrv, SweetAlert, DTOptionsBuilder, DTColumnDefBuilder, ngDialog) {
    var vm = this;
	vm.activeTab = "create";
	vm.profiles = [];
	vm.currentProfile = {};
	vm.operations = [];
	vm.newProfile = "";
	vm.newOperation = {
		cwd: "/",
		timeout: "30",
		daemon: false,
		script: false
	};
	vm.currentOperation = {};
	vm.environments = [];
	vm.templates = [];
	vm.currentEnvironment = {};
	vm.currentTemplate = "";
	vm.currentOperationName = "";
	vm.previousName = "";

	vm.updateProfiles = updateProfiles;


	vm.createProfile = createProfile;
	vm.deleteProfile = deleteProfile;
	vm.changeToConfigure = changeToConfigure;

	vm.getOperations = getOperations;
	vm.addOperationWindow = addOperationWindow;
	vm.operationInfo = operationInfo;
	vm.saveOperation = saveOperation;
	vm.editOperation = editOperation;
	vm.updateOperation = updateOperation;
	vm.deleteOperation = deleteOperation;
	vm.uploadHelper = uploadHelper;

	vm.executeOperation = executeOperation;
	vm.updateEnvironment = updateEnvironment;
	vm.output = "";

	$scope.session = {
		commands: [],
		output: [],
		$scope:$scope
	};


	$scope.command = "";
	$scope.$watch ("command", function() {
		console.log ("AAA");
	});

	// Init

	function updateProfiles() {
		genericSrv.listProfiles().success (function (data) {
			vm.profiles = data;
			vm.currentProfile = vm.profiles[0];
			genericSrv.listOperations (vm.currentProfile).success (function (data) {
				vm.operations = data;
				for (var i = 0; i < vm.operations.length; ++i) {
					vm.operations[i].commandName = window.atob (vm.operations[i].commandName);
				}
				vm.currentOperation = vm.operations[0];
				genericSrv.getEnvironments().success (function (data) {
					vm.environments = data;
					vm.currentEnvironment = vm.environments[0];
					for (var i = 0; i < vm.currentEnvironment.containers.length; ++i) {
						vm.currentEnvironment.containers[i].operation = vm.operations[0];
					}
					getTemplates();
					if (vm.environments.length === 0) {
						SweetAlert.swal ("ERROR!", "Please create environment first", "error");
					}
				}).error (function (error) {
					SweetAlert.swal ("ERROR!", "Environments error: " + error.replace(/\\n/g, " "), "error");
				});
			});
		});
	}
	updateProfiles();


	var getTemplates = function() {
		vm.templates = [];
		for (var i = 0; i < vm.currentEnvironment.containers.length; ++i) {
			if (!(vm.templates.indexOf (vm.currentEnvironment.containers[i].templateName) > -1)) {
				vm.templates.push (vm.currentEnvironment.containers[i].templateName);
			}
		}
		vm.currentTemplate = vm.templates[0];
	};


	// Create

	function createProfile() {
		if (vm.profiles.indexOf (vm.newProfile) > -1) {
			SweetAlert.swal ("ERROR!", "Profile already exists", "error");
		}
		else if (vm.newProfile === "") {
			SweetAlert.swal ("ERROR!", "Please enter profile name", "error");
		}
		else {
			genericSrv.saveProfile (vm.newProfile).success (function (data) {
				SweetAlert.swal ("Success!", "Your profile was created.", "success");
				vm.updateProfiles();
				vm.newProfile = "";
			}).error (function (error) {
				SweetAlert.swal ("ERROR!", "Profile create error: " + error.replace(/\\n/g, " "), "error");
			});
		}
	}


	function deleteProfile (profile) {
		SweetAlert.swal({
			title: "Are you sure?",
			text: "Your will not be able to recover this cluster!",
			type: "warning",
			showCancelButton: true,
			confirmButtonColor: "#ff3f3c",
			confirmButtonText: "Delete",
			cancelButtonText: "Cancel",
			closeOnConfirm: false,
			closeOnCancel: true,
			showLoaderOnConfirm: true
		},
		function (isConfirm) {
			if (isConfirm) {
				genericSrv.deleteProfile (profile).success (function (data) {
					SweetAlert.swal ("Success!", "Your profile was deleted.", "success");
					vm.updateProfiles();
				}).error (function (error) {
					SweetAlert.swal ("ERROR!", "Profile delete error: " + error.replace(/\\n/g, " "), "error");
				});
			}
		});
	}


	function changeToConfigure (profile) {
		if (typeof (profile) == "string") {
			profile = JSON.parse (vm.currentProfile);
		}
		vm.currentProfile = profile;
		vm.getOperations();
		console.log (vm.currentProfile);
		vm.activeTab = "configure";
	}


	// Configure

	function getOperations() {
		if (vm.currentProfile === {} || vm.currentProfile === "" || vm.currentProfile === undefined) {
			SweetAlert.swal ("ERROR!", "Please select profile", "error");
		}
		else {
			if (typeof (vm.currentProfile) == "string") {
				vm.currentProfile = JSON.parse (vm.currentProfile);
			}
			genericSrv.listOperations (vm.currentProfile).success (function (data) {
				vm.operations = data;
				for (var i = 0; i < vm.operations.length; ++i) {
					vm.operations[i].commandName = window.atob (vm.operations[i].commandName);
				}
				vm.currentOperation = vm.operations[0];
				console.log (vm.listOfOperations);
			});
		}
	}


	function addOperationWindow() {
		var id = ngDialog.open ({
			template: "plugins/generic/partials/addOperation.html",
			scope: $scope
		}).id;
		$scope.$on('ngDialog.opened', function (e, $dialog) {;
            document.getElementById ("uploadBtn").addEventListener ("change", readScript, false);
        });
	}

/*d 10 k 18*/
	function operationInfo (operation) {
		vm.currentOperation = operation;
		ngDialog.open ({
			template: "plugins/generic/partials/viewOperation.html",
			scope: $scope
		});
	}


	function checkIfExists(operation) {
		var arr = [];
		for (var i = 0; i < vm.operations.length; ++i) {
			arr.push (vm.operations.operationName);
		}
		if (arr.indexOf (operation.operationName) > -1) {
			return true;
		}
		return false;
	}


	function saveOperation() {
		if (vm.newOperation.operationName === "" || vm.newOperation.operationName === undefined) {
			SweetAlert.swal ("ERROR!", "Please enter operation name", "error");
		}
		else if (checkIfExists (vm.newOperation)) {
			SweetAlert.swal ("ERROR!", "Operation already exists", "error");
		}
		else if (vm.newOperation.cwd === "" || vm.newOperation.cwd === undefined) {
			SweetAlert.swal ("ERROR!", "Please enter CWD", "error");
		}
		else if (vm.newOperation.timeout === "" || vm.newOperation.timeout === undefined) {
			SweetAlert.swal ("ERROR!", "Please enter timeout", "error");
		}
		else {
			genericSrv.saveOperation (vm.currentProfile, vm.newOperation).success (function (data) {
				SweetAlert.swal ("Success!", "Your operation was created.", "success");
				vm.getOperations();
				vm.newOperation = {
					cwd: "/",
					timeout: "30",
					daemon: false,
					script: false
				};
				ngDialog.closeAll();
			}).error (function (error) {
				SweetAlert.swal ("ERROR!", "Operation create error: " + error.replace(/\\n/g, " "), "error");
			});
		}
	}


	function editOperation (operation) {
		vm.currentOperation = operation;
		vm.previousName = vm.currentOperation.operationName;
		ngDialog.open ({
			template: "plugins/generic/partials/editOperation.html",
			scope: $scope
		});
		$scope.$on('ngDialog.opened', function (e, $dialog) {
			document.getElementById ("uploadBtn").addEventListener ("change", readScript, false);
		});
	}

	function updateOperation() {
		if (vm.previousName !== vm.currentOperation.operationName) {
			if (checkIfExists (vm.currentOperation)) {
				SweetAlert.swal ("ERROR!", "Operation already exists", "error");
				return;
			}
		}
		if (vm.currentOperation.operationName === "" || vm.currentOperation.operationName === undefined) {
			SweetAlert.swal ("ERROR!", "Please enter operation name", "error");
		}
		else if (vm.currentOperation.cwd === "" || vm.currentOperation.cwd === undefined) {
			SweetAlert.swal ("ERROR!", "Please enter CWD", "error");
		}
		else if (vm.currentOperation.timeout === "" || vm.currentOperation.timeout === undefined) {
			SweetAlert.swal ("ERROR!", "Please enter timeout", "error");
		}
		genericSrv.updateOperation (vm.currentOperation).success (function (data) {
			SweetAlert.swal ("Success!", "Your operation was updated.", "success");
			vm.getOperations();
			ngDialog.closeAll();
		}).error (function (error) {
			SweetAlert.swal ("ERROR!", "Operation update error: " + error.replace(/\\n/g, " "), "error");
		});
	}

	function deleteOperation (operationId) {
		SweetAlert.swal({
			title: "Are you sure?",
			text: "Your will not be able to recover this cluster!",
			type: "warning",
			showCancelButton: true,
			confirmButtonColor: "#ff3f3c",
			confirmButtonText: "Delete",
			cancelButtonText: "Cancel",
			closeOnConfirm: false,
			closeOnCancel: true,
			showLoaderOnConfirm: true
		},
		function (isConfirm) {
			if (isConfirm) {
				genericSrv.deleteOperation (operationId).success (function (data) {
					SweetAlert.swal ("Success!", "Your operation was deleted.", "success");
					vm.getOperations();
					ngDialog.closeAll();
				}).error (function (error) {
					SweetAlert.swal ("ERROR!", "Operation delete error: " + error.replace(/\\n/g, " "), "error");
				});
			}
		});
	}


	function readScript (e) {
    	var file = e.target.files[0];
    	if (!file) {
    		return;
    	}
    	var reader = new FileReader();
    	reader.onload = function (e) {
    		var content = e.target.result;
    		vm.uploadHelper (content);
    	};
    	reader.readAsText (file);
    }


	function uploadHelper (content) {
		vm.newOperation.commandName = content;
		vm.newOperation.script = true;
	}


	// Manage


	function executeOperation (container) {
		vm.output = "";
		for (var i = 0; i < vm.currentEnvironment.containers.length; ++i) {
			if (vm.currentEnvironment.containers[i].id === container.id) {
				for (var j = 0; j < vm.operations.length; ++j) {
					if (vm.operations[j].operationName === vm.currentEnvironment.containers[i].operation) {
						vm.currentOperation = vm.operations[j];
						break;
					}
				}
			}
		}
		console.log (vm.currentOperation);
		genericSrv.executeOperation (vm.currentOperation.operationId, container.hostname, vm.currentEnvironment.id).success (function (data) {
			vm.output = data;
			$scope.$broadcast('terminal-output', {
				output: true,
				text: [vm.output],
				breakLine: true
			});
		}).error (function (error) {
			SweetAlert.swal ("ERROR!", "Operation execute error: " + error.replace(/\\n/g, " "), "error");
		});
	}


	function updateEnvironment() {
		vm.currentEnvironment = JSON.parse (vm.currentEnvironment);
		getTemplates();
		for (var i = 0; i < vm.currentEnvironment.containers.length; ++i) {
			vm.currentEnvironment.containers[i].operation = vm.operations[0];
		}
		vm.getOperations();
	}

	vm.info = {};
    genericSrv.getPluginInfo().success (function (data) {
    	vm.info = data;
    });
}
