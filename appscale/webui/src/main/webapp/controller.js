'use strict';

angular.module('subutai.plugins.appscale.controller', [])
	.controller('AppscaleCtrl', AppscaleCtrl)
	.directive('mSelect', initMSelect);

AppscaleCtrl.$inject = ['appscaleSrv', 'SweetAlert', '$scope', 'ngDialog', '$http'];


function AppscaleCtrl (appscaleSrv, SweetAlert, $scope, ngDialog, $http) {
	var vm = this;
	vm.config = {userDomain : ""};
	vm.nodes = [];
	vm.console = "";
	vm.activeTab = "install";
	vm.currentEnvironment = {};
	vm.environments = [];
	vm.currentCluster = {};
	vm.clusters = [];
	vm.hostnames = [];
	vm.config.scaleOption = "static";
	vm.hubRegister = false;

	vm.checked = false;

	function getContainers() {
		// TODO: get ip of master if appscale is already built
		appscaleSrv.getEnvironments().success (function (data) {
		    console.log (data);
			vm.environments = [];
			vm.nodes = [];
			vm.hostnames = [];
			for (var i = 0; i < data.length; ++i)
			{
				for (var j = 0; j < data[i].containers.length; ++j) {
					if (data[i].containers[j].templateName === "appscale271") {
						vm.environments.push (data[i]);
						break;
					}
				}
			}
			appscaleSrv.listClusters().success (function (data) {

				vm.clusters = data;
				vm.currentCluster = vm.clusters[0];
				var temp = [];
				var check = true;
				for (var i = 0; i < vm.environments.length; ++i) {
					for (var j = 0; j < vm.clusters.length; ++j) {
						if (vm.environments[i].id === vm.clusters[j].environmentId) {
							check = false;
							break;
						}
					}
					if (check) {
						temp.push (vm.environments[i]);
					}
				}
				vm.environments = temp;

				if (vm.environments.length === 0) {
					// @todo
					//SweetAlert.swal("ERROR!", 'No free environment. Create a new one', "error");
				}
				else {
					vm.currentEnvironment = vm.environments[0];
					for (var i = 0; i < vm.currentEnvironment.containers.length; ++i) {
						if (vm.currentEnvironment.containers[i].templateName === "appscale271") {
							vm.nodes.push (vm.currentEnvironment.containers [i]);

							if( vm.currentEnvironment.containers.length > 1 && i > 0 )
								vm.hostnames.push(vm.currentEnvironment.containers[i]);
						}
					}

					if( vm.hostnames.length == 0 && vm.currentEnvironment.containers.length > 0 )
					{
						vm.hostnames.push(vm.currentEnvironment.containers[0]);
					}

					vm.config.master = vm.nodes[0];

					vm.config.appeng = [];
					vm.config.zookeeper = [];
					vm.config.db = [];
					vm.config.environment = vm.currentEnvironment;
				}
			});
		});
	}

	$http.get(SERVER_URL + "rest/v1/hub/registration_state", {
		withCredentials: true,
		headers: {'Content-Type': 'application/json'}
	}).success(function (data) {
		vm.hubRegister = data.isRegisteredToHub;
	});


	getContainers();
	vm.changeNodes = changeNodes;
	function changeNodes() {
		vm.nodes = [];
		vm.hostnames = [];
		for (var i = 0; i < vm.currentEnvironment.containers.length; ++i) {
			if (vm.currentEnvironment.containers[i].templateName === "appscale271") {
				vm.nodes.push (vm.currentEnvironment.containers[i]);

				if( vm.currentEnvironment.containers.length > 1 && i > 0 )
					vm.hostnames.push(vm.currentEnvironment.containers[i]);
			}
		}
		if( vm.hostnames.length == 0 && vm.currentEnvironment.containers.length > 0 )
		{
			vm.hostnames.push(vm.currentEnvironment.containers[0]);
		}

		vm.config.master = vm.nodes[0];

		vm.config.appeng = [];
		vm.config.zookeeper = [];
		vm.config.db = [];
		vm.config.environment = vm.currentEnvironment;
	}

	function listClusters() {
		appscaleSrv.listClusters().success (function (data) {
			vm.clusters = data;
			vm.currentCluster = vm.clusters[0];
			var temp = [];
			var check = true;
			for (var i = 0; i < vm.environments.length; ++i) {
				for (var j = 0; j < vm.clusters.length; ++j) {
					if (vm.environments[i].id === vm.clusters[j].environmentId) {
						check = false;
						break;
					}
				}
				if (check) {
					temp.push (vm.environments[i]);
				}
			}
			vm.environments = temp;
			if (vm.environments.length === 0) {
				// @todo
				//SweetAlert.swal("ERROR!", 'No free environment. Create a new one', "error");
			}
			else {
				vm.currentEnvironment = vm.environments[0];
				for (var i = 0; i < vm.currentEnvironment.containers.length; ++i) {
					if (vm.currentEnvironment.containers[i].templateName === "appscale271") {
						vm.nodes.push (vm.currentEnvironment.containers [i]);

						if( vm.currentEnvironment.containers.length > 1 && i > 0 )
							vm.hostnames.push(vm.currentEnvironment.containers[i]);
					}
				}

				if( vm.hostnames.length == 0 && vm.currentEnvironment.containers.length > 0 )
				{
					vm.hostnames.push(vm.currentEnvironment.containers[0]);
				}

				vm.config.master = vm.nodes[0];

				vm.config.appeng = [];
				vm.config.zookeeper = [];
				vm.config.db = [];
				vm.config.environment = vm.currentEnvironment;
			}
		});
	}


	function wrongDomain() {
		if (vm.config.userDomain.match (/([a-z]+)\.([a-z][a-z]+)/) === null) {
			return true;
		}
		else {
			return false;
		}
	}

	vm.build = build;
	function build() {
        console.log (vm.config);
		if (vm.config.userDomain === "") {
			SweetAlert.swal ("ERROR!", 'Please enter domain', "error");
		}
		else if (wrongDomain() && vm.config.domainOption == 0 ) {
			SweetAlert.swal ("ERROR!", 'Wrong domain format', "error");
		}
		else {
			LOADING_SCREEN();
			appscaleSrv.build (vm.config).success (function (data) {
				LOADING_SCREEN ('none');
				SweetAlert.swal ("Success!", "Your Appscale cluster was created.", "success");
				listClusters();
			}).error (function (error) {
				LOADING_SCREEN ('none');
				SweetAlert.swal ("ERROR!", 'Appscale build error: ' + error.replace(/\\n/g, ' '), "error");
			});
		}
	}


	vm.getClustersInfo = getClustersInfo;
	function getClustersInfo (selectedCluster) {
		LOADING_SCREEN();
		appscaleSrv.getClusterInfo(selectedCluster).success(function (data) {
			LOADING_SCREEN ('none');
			vm.currentCluster = data;
		});
	}

	vm.uninstallCluster = uninstallCluster;
	function uninstallCluster() {
		LOADING_SCREEN();
		console.log (vm.currentCluster);
		appscaleSrv.uninstallCluster (vm.currentCluster).success (function (data) {
			LOADING_SCREEN ('none');
			SweetAlert.swal ("Success!", "Your Appscale cluster is being deleted.", "success");
			listClusters();
		}).error (function (error) {
			LOADING_SCREEN ('none');
			SweetAlert.swal ("ERROR!", 'Appscale delete error: ' + error.replace(/\\n/g, ' '), "error");
		});
	}

	vm.toggleScale = function( val ) {
		vm.checked = val;
		if( vm.checked  )
			vm.config.scaleOption = "scale";
		else
			vm.config.scaleOption = "static";
	};

	vm.quickInstallPopup = function( val )
	{
		ngDialog.open({
			template: 'plugins/appscale/partials/quick-install.html',
			scope: $scope
		});
	};

	vm.quickInstall = function( val )
	{
		LOADING_SCREEN();
		ngDialog.close();
		appscaleSrv.quickInstall( val ).success(function(data) {
			SweetAlert.swal ("Success!", "Your Appscale cluster '"+ val.name +"' is created.", "success");
			listClusters();
			LOADING_SCREEN('none');
		}).error( function (data) {
			SweetAlert.swal ("ERROR!", data, "error");
			LOADING_SCREEN('none');
		} );
	}

    function arrayObjectIndexOf(myArray, searchTerm, property) {
        for(var i = 0, len = myArray.length; i < len; i++) {
            if (myArray[i][property] === searchTerm) return i;
        }
        return -1;
    }

	vm.controllerMod = function ( hostname ) {
		if( vm.nodes.length > 0 )
		{
			$('a[ng-click="deselectAll()"]').click();

			vm.hostnames = [];
			for( var i = 0; i < vm.nodes.length; i++ )
			{
				vm.hostnames.push(vm.nodes[i]);
			}
			console.log (vm.hostnames);
            var index = arrayObjectIndexOf (vm.hostnames, vm.config.master.hostname, "hostname");

			if (index > -1) {
				vm.hostnames.splice(index, 1);
			}
		}
	}
}

function initMSelect()
{
	var controller = ['$scope', function ($scope) {

		$scope.selected = [];

		$scope.select = function( id )
		{
			$scope.selected.push( id );
		};

		$scope.deselect = function( id )
		{
			var idx = $scope.selected.indexOf(id);
			if( idx > -1 )
			{
				$scope.selected.splice(idx, 1);
			}
		};

		$scope.selectAll = function( )
		{
			$scope.selected = $scope.selected.concat( $scope.items );
		};

		$scope.deselectAll = function( )
		{
			$scope.selected = [];
		};
	}];

	return {
		restrict: 'E',
		scope: {
			items: '=',
			selected: '='
		},
		templateUrl : 'plugins/appscale/directives/m-select.html',
		controller : controller
	}
}
