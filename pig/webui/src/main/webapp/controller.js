'use strict';

angular.module('subutai.plugins.pig.controller', [])
    .controller('PigCtrl', PigCtrl)
	.directive('colSelectPigNodes', colSelectPigNodes);

PigCtrl.$inject = ['$scope', 'pigSrv', 'SweetAlert', 'DTOptionsBuilder', 'DTColumnDefBuilder', 'ngDialog'];

function PigCtrl($scope, pigSrv, SweetAlert, DTOptionsBuilder, DTColumnDefBuilder, ngDialog) {
    var vm = this;
	vm.activeTab = 'install';
	vm.pigInstall = {};
	vm.clusters = [];
	vm.hadoopClusters = [];
	vm.currentClusterNodes = [];
	vm.currentCluster = [];
	vm.availableNodes = [];

	//functions
	vm.getClustersInfo = getClustersInfo;	
	vm.getHadoopClusterNodes = getHadoopClusterNodes;	
	vm.addContainer = addContainer;	
	vm.createPig = createPig;	
	vm.deleteNode = deleteNode;
	vm.addNodeForm = addNodeForm;
	vm.addNode = addNode;
	vm.deleteCluster = deleteCluster;

	pigSrv.getHadoopClusters().success(function(data){
		vm.hadoopClusters = data;
		console.log(vm.hadoopClusters);
		if(vm.hadoopClusters.length == 0) {
			SweetAlert.swal("ERROR!", 'No Hadoop clusters was found! Create Hadoop cluster first.', "error");
		}
	}).error(function(error){
		SweetAlert.swal("ERROR!", 'No Hadoop clusters was found! ERROR: ' + error.replace(/\\n/g, ' '), "error");
	});
	setDefaultValues();

	function getClusters() {
	    LOADING_SCREEN();
		pigSrv.getClusters().success(function (data) {
			vm.clusters = data;
			LOADING_SCREEN("none");
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
		pigSrv.getClusters(selectedCluster).success(function (data) {
			vm.currentCluster = data;
			LOADING_SCREEN('none');
		}).error(function(error){
			SweetAlert.swal("ERROR!", 'Call cluster error: ' + error.replace(/\\n/g, ' '), "error");
			LOADING_SCREEN('none');
		});
	}

	function addNodeForm() {
		if(vm.currentCluster.clusterName === undefined) return;
		LOADING_SCREEN();
		pigSrv.getAvailableNodes(vm.currentCluster.clusterName).success(function (data) {
			vm.availableNodes = data;
			LOADING_SCREEN('none');
		});
		ngDialog.open({
			template: 'plugins/pig/partials/addNodesForm.html',
			scope: $scope
		});
	}

	function addNode(chosenNode) {
		if(chosenNode === undefined) return;
		if(vm.currentCluster.clusterName === undefined) return;
		LOADING_SCREEN();
		SweetAlert.swal("Success!", "Adding node action started.", "success");
		ngDialog.closeAll();
		pigSrv.addNode(vm.currentCluster.clusterName, chosenNode).success(function (data) {
			SweetAlert.swal(
				"Success!",
				"Node has been added on cluster " + vm.currentCluster.clusterName + ".",
				"success"
			);
			getClustersInfo(vm.currentCluster.clusterName);
			LOADING_SCREEN('none');
		});
	}

	function getHadoopClusterNodes(selectedCluster) {
		LOADING_SCREEN();
		pigSrv.getHadoopClusters(selectedCluster).success(function (data) {
			vm.currentClusterNodes = data.slaves;
			vm.currentClusterNodes.push(data.nameNode);
			LOADING_SCREEN('none');
		});
	}

	function createPig() {
		if(vm.pigInstall.clusterName === undefined || vm.pigInstall.clusterName.length == 0) return;
		if(vm.pigInstall.hadoopClusterName === undefined || vm.pigInstall.hadoopClusterName.length == 0) return;

		SweetAlert.swal("Success!", "Pig cluster start creating.", "success");
		LOADING_SCREEN();
		pigSrv.createPig(vm.pigInstall).success(function (data) {
			SweetAlert.swal("Success!", "Your pig cluster start creating.", "success");
			LOADING_SCREEN("none");
			getClusters();
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Pig cluster create error: ' + error.replace(/\\n/g, ' '), "error");
			LOADING_SCREEN("none");
			getClusters();
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
				pigSrv.deleteCluster(vm.currentCluster.clusterName).success(function (data) {
					SweetAlert.swal("Deleted!", "Cluster has been deleted.", "success");
					vm.currentCluster = {};
					getClusters();
				}).error(function(error){
					SweetAlert.swal("ERROR!", 'Delete cluster error: ' + data.replace(/\\n/g, ' '), "error");
				});
			}
		});
	}

	function deleteNode(nodeId) {
		if(vm.currentCluster.clusterName === undefined) return;
		SweetAlert.swal({
			title: "Are you sure?",
			text: "This operation removes the Pig node from the cluster, and does not delete the container itself.",
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
				pigSrv.deleteNode(vm.currentCluster.clusterName, nodeId).success(function (data) {
					SweetAlert.swal("Deleted!", "Node has been removed.", "success");
					getClustersInfo(vm.currentCluster.clusterName);
				});
			}
		});
	}

	function addContainer(containerId) {
		if(vm.pigInstall.nodes.indexOf(containerId) > -1) {
			vm.pigInstall.nodes.splice(vm.pigInstall.nodes.indexOf(containerId), 1);
		} else {
			vm.pigInstall.nodes.push(containerId);
		}
	}

	function setDefaultValues() {
		vm.pigInstall = {};
		vm.pigInstall.nodes = [];
	}

	vm.info = {};
    pigSrv.getPluginInfo().success (function (data) {
    	vm.info = data;
    });
}

function colSelectPigNodes() {
	return {
		restrict: 'E',
		templateUrl: 'plugins/pig/directives/col-select/col-select-containers.html'
	}
};

