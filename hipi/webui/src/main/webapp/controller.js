'use strict';

angular.module('subutai.plugins.hipi.controller', [])
    .controller('HipiCtrl', HipiCtrl)
	.directive('colSelectHipiNodes', colSelectHipiNodes);

HipiCtrl.$inject = ['$scope', 'hipiSrv', 'SweetAlert', 'DTOptionsBuilder', 'DTColumnDefBuilder', 'ngDialog'];

function HipiCtrl($scope, hipiSrv, SweetAlert, DTOptionsBuilder, DTColumnDefBuilder, ngDialog) {
    var vm = this;
	vm.activeTab = 'install';
	vm.hipiInstall = {};
	vm.clusters = [];
	vm.hadoopClusters = [];
	vm.currentClusterNodes = [];
	vm.currentCluster = [];
	vm.availableNodes = [];

	//functions
	vm.getClustersInfo = getClustersInfo;	
	vm.getHadoopClusterNodes = getHadoopClusterNodes;	
	vm.addContainer = addContainer;	
	vm.createHipi = createHipi;	
	vm.deleteNode = deleteNode;
	vm.addNodeForm = addNodeForm;
	vm.addNode = addNode;
	vm.deleteCluster = deleteCluster;

	hipiSrv.getHadoopClusters().success(function(data){
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
		hipiSrv.getClusters().success(function (data) {
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
		hipiSrv.getClusters(selectedCluster).success(function (data) {
			vm.currentCluster = data;
			LOADING_SCREEN('none');
		});
	}

	function addNodeForm() {
		if(vm.currentCluster.clusterName === undefined) return;
		hipiSrv.getAvailableNodes(vm.currentCluster.clusterName).success(function (data) {
			vm.availableNodes = data;
			console.log(vm.availableNodes);
		});
		ngDialog.open({
			template: 'plugins/hipi/partials/addNodesForm.html',
			scope: $scope
		});
	}

	function addNode(chosenNode) {
		if(chosenNode === undefined) return;
		if(vm.currentCluster.clusterName === undefined) return;
		SweetAlert.swal("Success!", "Adding node action started.", "success");
		ngDialog.closeAll();
		hipiSrv.addNode(vm.currentCluster.clusterName, chosenNode).success(function (data) {
			SweetAlert.swal(
				"Success!",
				"Node has been added on cluster " + vm.currentCluster.clusterName + ".",
				"success"
			);
			getClustersInfo(vm.currentCluster.clusterName);
		}).error(function(error){
			SweetAlert.swal("ERROR!", 'Adding node error: ' + error.replace(/\\n/g, ' '), "error");
		});
	}

	function getHadoopClusterNodes(selectedCluster) {
		LOADING_SCREEN();
		hipiSrv.getHadoopClusters(selectedCluster).success(function (data) {
			vm.currentClusterNodes.push(data.nameNode);
			LOADING_SCREEN('none');
			console.log(vm.currentClusterNodes);
		});
	}

	function createHipi() {
		if(vm.hipiInstall.clusterName === undefined || vm.hipiInstall.clusterName.length == 0) return;
		if(vm.hipiInstall.hadoopClusterName === undefined || vm.hipiInstall.hadoopClusterName.length == 0) return;

		SweetAlert.swal("Success!", "Hipi cluster start creating.", "success");
		LOADING_SCREEN();
		hipiSrv.createHipi(vm.hipiInstall).success(function (data) {
			SweetAlert.swal("Success!", "Hipi cluster created successfully", "success");
			LOADING_SCREEN("none");
			getClusters();
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Hipi cluster create error: ' + error.replace(/\\n/g, ' '), "error");
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
				hipiSrv.deleteCluster(vm.currentCluster.clusterName).success(function (data) {
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
			text: "This operation removes the Hipi node from the cluster, and does not delete the container itself.",
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
				hipiSrv.deleteNode(vm.currentCluster.clusterName, nodeId).success(function (data) {
					SweetAlert.swal("Deleted!", "Node has been removed.", "success");
					getClustersInfo(vm.currentCluster.clusterName);
				}).error(function(error){
					SweetAlert.swal("ERROR!", 'Delete node error: ' + data.replace(/\\n/g, ' '), "error");
				});
			}
		});
	}

	function addContainer(containerId) {
		if(vm.hipiInstall.nodes.indexOf(containerId) > -1) {
			vm.hipiInstall.nodes.splice(vm.hipiInstall.nodes.indexOf(containerId), 1);
		} else {
			vm.hipiInstall.nodes.push(containerId);
		}
	}

	function setDefaultValues() {
		vm.hipiInstall = {};
		vm.hipiInstall.nodes = [];
	}

	vm.info = {};
    hipiSrv.getPluginInfo().success (function (data) {
    	vm.info = data;
    });
}

function colSelectHipiNodes() {
	return {
		restrict: 'E',
		templateUrl: 'plugins/hipi/directives/col-select/col-select-containers.html'
	}
};

