'use strict';

angular.module('subutai.plugins.mahout.controller', [])
    .controller('MahoutCtrl', MahoutCtrl)
	.directive('colSelectMahoutNodes', colSelectMahoutNodes);

MahoutCtrl.$inject = ['$scope', 'mahoutSrv', 'SweetAlert', 'DTOptionsBuilder', 'DTColumnDefBuilder', 'ngDialog'];

function MahoutCtrl($scope, mahoutSrv, SweetAlert, DTOptionsBuilder, DTColumnDefBuilder, ngDialog) {
    var vm = this;
	vm.activeTab = 'install';
	vm.mahoutInstall = {};
	vm.clusters = [];
	vm.hadoopClusters = [];
	vm.currentClusterNodes = [];
	vm.currentCluster = [];
	vm.availableNodes = [];

	//functions
	vm.getClustersInfo = getClustersInfo;	
	vm.getHadoopClusterNodes = getHadoopClusterNodes;	
	vm.addContainer = addContainer;	
	vm.createMahout = createMahout;	
	vm.deleteNode = deleteNode;
	vm.addNodeForm = addNodeForm;
	vm.addNode = addNode;
	vm.deleteCluster = deleteCluster;

	mahoutSrv.getHadoopClusters().success(function(data){
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
		mahoutSrv.getClusters().success(function (data) {
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
		mahoutSrv.getClusters(selectedCluster).success(function (data) {
			vm.currentCluster = data;
			LOADING_SCREEN('none');
		});
	}

	function addNodeForm() {
		if(vm.currentCluster.clusterName === undefined) return;
		LOADING_SCREEN();
		mahoutSrv.getAvailableNodes(vm.currentCluster.clusterName).success(function (data) {
			LOADING_SCREEN('none');
			vm.availableNodes = data;
		});
		ngDialog.open({
			template: 'plugins/mahout/partials/addNodesForm.html',
			scope: $scope
		});
	}

	function addNode(chosenNode) {
		if(chosenNode === undefined) return;
		if(vm.currentCluster.clusterName === undefined) return;
		LOADING_SCREEN();
		SweetAlert.swal("Success!", "Adding node action started.", "success");
		ngDialog.closeAll();
		mahoutSrv.addNode(vm.currentCluster.clusterName, chosenNode).success(function (data) {
			SweetAlert.swal(
				"Success!",
				"Node has been added on cluster " + vm.currentCluster.clusterName + ".",
				"success"
			);
			getClustersInfo(vm.currentCluster.clusterName);
			LOADING_SCREEN('none');
		}).error(function(error){
			SweetAlert.swal("ERROR!", 'Mahout add node error: ' + error.replace(/\\n/g, ' '), "error");
		});
	}

	function getHadoopClusterNodes(selectedCluster) {
		LOADING_SCREEN();
		mahoutSrv.getHadoopClusters(selectedCluster).success(function (data) {
			vm.currentClusterNodes = data.slaves;
			vm.currentClusterNodes.push(data.nameNode);
			LOADING_SCREEN('none');
		});
	}

	function createMahout() {
		if(vm.mahoutInstall.clusterName === undefined || vm.mahoutInstall.clusterName.length == 0) return;
		if(vm.mahoutInstall.hadoopClusterName === undefined || vm.mahoutInstall.hadoopClusterName.length == 0) return;

		SweetAlert.swal("Success!", "Mahout cluster started creating.", "success");
		LOADING_SCREEN();
		mahoutSrv.createMahout(vm.mahoutInstall).success(function (data) {
			SweetAlert.swal("Success!", "Mahout cluster was created.", "success");
			LOADING_SCREEN("none");
			getClusters();
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Mahout cluster create error: ' + error.replace(/\\n/g, ' '), "error");
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
				mahoutSrv.deleteCluster(vm.currentCluster.clusterName).success(function (data) {
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
			text: "This operation removes the Mahout node from the cluster, and does not delete the container itself.",
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
				mahoutSrv.deleteNode(vm.currentCluster.clusterName, nodeId).success(function (data) {
					SweetAlert.swal("Deleted!", "Node has been removed.", "success");
					getClustersInfo(vm.currentCluster.clusterName);
				}).error(function(error){
					SweetAlert.swal("ERROR!", 'Delete node error: ' + error.replace(/\\n/g, ' '), "error");
				});
			}
		});
	}

	function addContainer(containerId) {
		if(vm.mahoutInstall.nodes.indexOf(containerId) > -1) {
			vm.mahoutInstall.nodes.splice(vm.mahoutInstall.nodes.indexOf(containerId), 1);
		} else {
			vm.mahoutInstall.nodes.push(containerId);
		}
	}

	function setDefaultValues() {
		vm.mahoutInstall = {};
		vm.mahoutInstall.nodes = [];
	}

	vm.info = {};
    mahoutSrv.getPluginInfo().success (function (data) {
    	vm.info = data;
    });
}

function colSelectMahoutNodes() {
	return {
		restrict: 'E',
		templateUrl: 'plugins/mahout/directives/col-select/col-select-containers.html'
	}
};

