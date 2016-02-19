'use strict';

angular.module('subutai.plugins.appscale.controller', [])
        .controller('AppscaleCtrl', AppscaleCtrl);

AppscaleCtrl.$inject = ['appscaleSrv', 'SweetAlert', '$scope', 'ngDialog'];


function AppscaleCtrl (appscaleSrv, SweetAlert, $scope, ngDialog) {
	var vm = this;
	vm.config = {};
	vm.nodes = [];
	vm.console = "";

	function getContainers() {
		// TODO: get ip of master if appscale is already built
		appscaleSrv.getEnvironments().success (function (data) {
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
				SweetAlert.swal("ERROR!", 'Please create environment first, "error");
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
			}
		});
	}
	//getContainers();

	function changeNodes() {
		vm.nodes = [];
		for (var i = 0; i < vm.currentEnvironment.containers.length; ++i) {
			if (vm.currentEnvironment.containers[i].templateName === "appscale") {
				vm.nodes.push (vm.currentEnvironment.containers[i]);
			}
		}
	}


	function build() {
		appscaleSrv.build (config).success (function (data) {
			SweetAlert.swal ("Success!", "Your Appscale cluster is being created.", "success");
			vm.console = vm.config.master.hostname;
		}).error (function (error) {
			SweetAlert.swal ("ERROR!", 'Appscale build error: ' + error.replace(/\\n/g, ' '), "error");
		});
	}
}