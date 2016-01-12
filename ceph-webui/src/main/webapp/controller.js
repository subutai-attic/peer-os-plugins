"use strict";
angular.module("subutai.plugins.ceph.controller", [])
	.controller("CephCtrl", CephCtrl);

CephCtrl.$inject = ["$scope", "cephSrv", "SweetAlert", "ngDialog"];
// TODO: properly bind with environmentService
function CephCtrl($scope, cephSrv, SweetAlert, ngDialog) {
	var vm = this;
	vm.loading = false;
	vm.environments = [];
	vm.nodes = [];
	vm.currentEnvironment = {};
	vm.radosGWNode = {};
	vm.clusterName = "";

	vm.createEnvironment = createEnvironment;
	vm.changeNodes = changeNodes;

	cephSrv.getEnvironments().success (function (data) {
		console.log (data);
		for (var i = 0; i < data.length; ++i)
		{
			var added = false;
			for (var j = 0; j < data[i].containers.length; ++j) {
				if (data[i].containers[j].templateName === "ceph" && !added) {
					vm.environments.push (data[i]);
					added = true;
				}
				vm.nodes.push (data[i].containers[j]);
			}
		}
		vm.currentEnvironment = vm.environments[0];
		vm.nodes = vm.currentEnvironment.containers;
		vm.radosGWNode = vm.nodes[0];
	});

	function changeNodes() {
		vm.nodes = [];
		for (var i = 0; i < vm.currentEnvironment.containers.length; ++i) {
			if (vm.currentEnvironment.containers[i].templateName === "ceph") {
				vm.nodes.push (vm.currentEnvironment.containers[i]);
			}
		}
		vm.radosGWNode = vm.nodes[0];
	}


	function createEnvironment() {
		// TODO: summon dance
		vm.loading = true;
		// TODO: proper call
		cephSrv.createEnvironment (vm.clusterName, vm.currentEnvironment.id, vm.radosGWNode.hostname).success (function (data) {
			// TODO: release dance
			console.log (data);
			vm.loading = false;
			SweetAlert.swal ("Success!", "Your environment was created.", "success");
		}).error (function (error) {
			vm.loading = false;
			SweetAlert.swal ("ERROR!", "Environment create error: " + error.replace(/\\n/g, " "), "error");
		});
	}
}
