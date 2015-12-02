'use strict';

angular.module('subutai.plugins.shark.controller', [])
    .controller('SharkCtrl', SharkCtrl)
	.directive('colSelectContainers', colSelectContainers);

SharkCtrl.$inject = ['$scope', 'sharkSrv', 'SweetAlert', 'DTOptionsBuilder', 'DTColumnDefBuilder', 'ngDialog'];

function SharkCtrl($scope, sharkSrv, SweetAlert, DTOptionsBuilder, DTColumnDefBuilder, ngDialog) {
    var vm = this;
	vm.activeTab = 'install';
	vm.sharkInstall = {};
	vm.clusters = [];
	vm.sparkClusters = [];
	vm.currentClusterNodes = [];
	vm.currentCluster = {};
	vm.availableNodes = [];

	//functions
	vm.getClustersInfo = getClustersInfo;	
	vm.getSparkClusterNodes = getSparkClusterNodes;
	vm.createShark = createShark;
	vm.deleteNode = deleteNode;
	vm.addNodeForm = addNodeForm;
	vm.addNode = addNode;
	vm.deleteCluster = deleteCluster;
	vm.startServer = startServer;
	vm.stopServer = stopServer;

	sharkSrv.getSparkClusters().success(function(data){
		vm.sparkClusters = data;
		if(vm.sparkClusters.length == 0) {
			SweetAlert.swal("ERROR!", 'No Spark clusters were found! Create Spark cluster first.', "error");
		}
	}).error(function(error){
		SweetAlert.swal("ERROR!", 'No Spark clusters ware found! ERROR: ' + error.replace(/\\n/g, ' '), "error");
	});
	setDefaultValues();

	function getClusters() {
		sharkSrv.getClusters().success(function (data) {
			vm.clusters = data;
		});
	}
	getClusters();

	vm.dtOptions = DTOptionsBuilder
		.newOptions()
		.withOption('order', [[0, "asc" ]])
		.withOption('stateSave', true)
		.withPaginationType('full_numbers');

	vm.dtColumnDefs = [
		DTColumnDefBuilder.newColumnDef(0),
		DTColumnDefBuilder.newColumnDef(1),
		DTColumnDefBuilder.newColumnDef(2).notSortable()
	];

	function getClustersInfo(selectedCluster) {
		LOADING_SCREEN();
		sharkSrv.getClusters(selectedCluster).success(function (data) {
			vm.currentCluster = data;
			console.log (vm.currentCluster);
			LOADING_SCREEN('none');
		});
	}

	function addNodeForm() {
		if(vm.currentCluster.clusterName === undefined) return;
		sharkSrv.getAvailableNodes(vm.currentCluster.clusterName).success(function (data) {
			vm.availableNodes = data;
		});
		ngDialog.open({
			template: 'plugins/shark/partials/addNodesForm.html',
			scope: $scope
		});
	}

	function addNode(chosenNode) {
		if(chosenNode === undefined) return;
		if(vm.currentCluster.clusterName === undefined) return;
		SweetAlert.swal("Success!", "Node is being added.", "success");
		ngDialog.closeAll();
		sharkSrv.addNode(vm.currentCluster.clusterName, chosenNode).success(function (data) {
			SweetAlert.swal(
				"Success!",
				"Node has been added to cluster " + vm.currentCluster.clusterName + ".",
				"success"
			);
			getClustersInfo(vm.currentCluster.clusterName);
		});
	}


	function getSparkClusterNodes(selectedCluster) {
		LOADING_SCREEN();
		sharkSrv.getSparkClusters(selectedCluster).success(function (data) {
			vm.currentClusterNodes = data.slaveIds;
			vm.currentClusterNodes.push (data.masterNodeId);
			LOADING_SCREEN('none');
		}).error(function(error){
			SweetAlert.swal("ERROR!", error.replace(/\\n/g, ' '), "error");
			LOADING_SCREEN('none');
		});
	}

	function createShark() {
		console.log (vm.sharkInstall);
		if(vm.sharkInstall.clusterName === undefined || vm.sharkInstall.clusterName.length == 0) return;
		if(vm.sharkInstall.sparkClusterName === undefined || vm.sharkInstall.sparkClusterName.length == 0) return;
		SweetAlert.swal("Success!", "Shark cluster is being created.", "success");
		sharkSrv.createShark(vm.sharkInstall).success(function (data) {
			SweetAlert.swal("Success!", "Your Shark cluster has been successfully created.", "success");
			getClusters();
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Shark cluster creation error: ' + error.replace(/\\n/g, ' '), "error");
		});
		setDefaultValues();
		vm.activeTab = 'manage';
	}

	function deleteCluster() {
		if(vm.currentCluster.clusterName === undefined) return;
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
				sharkSrv.deleteCluster(vm.currentCluster.clusterName).success(function (data) {
					SweetAlert.swal("Deleted!", "Cluster has been deleted.", "success");
					vm.currentCluster = {};
					getClusters();
				}).error(function(error){
					SweetAlert.swal("ERROR!", 'Delete cluster error: ' + error.replace(/\\n/g, ' '), "error");
				});
			}
		});
	}

	function deleteNode(nodeId) {
		if(vm.currentCluster.clusterName === undefined) return;
		SweetAlert.swal({
			title: "Are you sure?",
			text: "Your will not be able to recover this node!",
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
				sharkSrv.deleteNode(vm.currentCluster.clusterName, nodeId).success(function (data) {
					SweetAlert.swal("Deleted!", "Node has been deleted.", "success");
					getClustersInfo(vm.currentCluster.clusterName);
				}).error(function(error){
					SweetAlert.swal("ERROR!", 'Delete cluster node error: ' + error.replace(/\\n/g, ' '), "error");
				});
			}
		});
	}


	function setDefaultValues() {
		vm.sharkInstall = {};
		vm.sharkInstall.clients = [];
	}


	function startServer() {
		if(vm.currentCluster.clusterName === undefined) return;
		vm.currentCluster.server.status = 'STARTING';
		sharkSrv.startNode (vm.currentCluster.clusterName, vm.currentCluster.server.hostname).success (function (data) {
			SweetAlert.swal("Success!", "Your server has been started.", "success");
			vm.currentCluster.server.status = 'RUNNING';
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Shark server failed to run cluster! error: ' + error.replace(/\\n/g, ' '), "error");
			vm.currentCluster.server.status = 'ERROR';
		});
	}


	function stopServer() {
		if(vm.currentCluster.clusterName === undefined) return;
		vm.currentCluster.server.status = 'STOPPING';
		sharkSrv.stopNode (vm.currentCluster.clusterName, vm.currentCluster.server.hostname).success (function (data) {
			SweetAlert.swal("Success!", "Your server has been stopped.", "success");
			vm.currentCluster.server.status = 'STOPPED';
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Shark server failed to stop cluster error: ' + error.replace(/\\n/g, ' '), "error");
			vm.currentCluster.server.status = 'ERROR';
		});
	}
}

function colSelectContainers() {
	return {
		restrict: 'E',
		templateUrl: 'plugins/shark/directives/col-select/col-select-containers.html'
	}
};

