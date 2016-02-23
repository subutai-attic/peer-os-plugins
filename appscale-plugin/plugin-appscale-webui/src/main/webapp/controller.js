'use strict';

angular.module('subutai.plugins.appscale.controller', [])
        .controller('AppscaleCtrl', AppscaleCtrl);

AppscaleCtrl.$inject = ['appscaleSrv', 'SweetAlert', '$scope', 'ngDialog'];


function AppscaleCtrl (appscaleSrv, SweetAlert, $scope, ngDialog) {
	var vm = this;
	vm.config = {};
	vm.nodes = [];
	vm.console = "";
	vm.activeTab = "install";
	vm.currentEnvironment = {};
	vm.environments = [];
	vm.currentCluster = {};
	vm.clusters = [];
	function getContainers() {
		// TODO: get ip of master if appscale is already built
		appscaleSrv.getEnvironments().success (function (data) {
            console.log (data);
            vm.environments = [];
			for (var i = 0; i < data.length; ++i)
			{
				for (var j = 0; j < data[i].containers.length; ++j) {
					if (data[i].containers[j].templateName === "appscale") {
						vm.environments.push (data[i]);
						break;
					}
				}
			}

			if (vm.environments.length === 0) {
				SweetAlert.swal("ERROR!", 'Please create environment first', "error");
			}
			else {
				vm.currentEnvironment = vm.environments[0];
				for (var i = 0; i < vm.currentEnvironment.containers.length; ++i) {
					if (vm.currentEnvironment.containers[i].templateName === "appscale") {
						vm.nodes.push (vm.currentEnvironment.containers [i]);
					}
				}
				vm.config.master = vm.nodes[0];
				vm.config.zookeeper = vm.nodes[0];
				vm.config.db = vm.nodes[0];
				vm.config.environment = vm.currentEnvironment;
			}
		});
	}


	getContainers();
    vm.changeNodes = changeNodes;
	function changeNodes() {
		vm.nodes = [];
		for (var i = 0; i < vm.currentEnvironment.containers.length; ++i) {
			if (vm.currentEnvironment.containers[i].templateName === "appscale") {
				vm.nodes.push (vm.currentEnvironment.containers[i]);
			}
		}
		vm.config.master = vm.nodes[0];
		vm.config.zookeeper = vm.nodes[0];
		vm.config.db = vm.nodes[0];
		vm.config.environment = vm.currentEnvironment;
	}

	function listClusters() {
		appscaleSrv.listClusters().success (function (data) {
			console.log (data);
			vm.clusters = data;
			vm.currentCluster = vm.clusters[0];
		});
	}
	listClusters();

    vm.build = build;
	function build() {
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
}