'use strict';

angular.module('subutai.plugins.nutch.controller', [])
    .controller('NutchCtrl', NutchCtrl)
	.directive('colSelectNutchNodes', colSelectNutchNodes);

NutchCtrl.$inject = ['$scope', 'nutchSrv', 'SweetAlert', 'DTOptionsBuilder', 'DTColumnDefBuilder', 'ngDialog'];

function NutchCtrl($scope, nutchSrv, SweetAlert, DTOptionsBuilder, DTColumnDefBuilder, ngDialog) {
    var vm = this;
	vm.activeTab = 'install';
	vm.nutchInstall = {};
	vm.clusters = [];
	vm.hadoopClusters = [];
	vm.currentClusterNodes = [];
	vm.currentCluster = [];
	vm.availableNodes = [];

	//functions
	vm.getClustersInfo = getClustersInfo;	
	vm.getHadoopClusterNodes = getHadoopClusterNodes;	
	vm.addContainer = addContainer;	
	vm.createNutch = createNutch;
	vm.deleteNode = deleteNode;
	vm.addNodeForm = addNodeForm;
	vm.addNode = addNode;
	vm.deleteCluster = deleteCluster;

	nutchSrv.getHadoopClusters().success(function(data){
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
		nutchSrv.getClusters().success(function (data) {
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
		nutchSrv.getClusters(selectedCluster).success(function (data) {
			vm.currentCluster = data;
			LOADING_SCREEN('none');
		});
	}

	function addNodeForm() {
		if(vm.currentCluster.clusterName === undefined) return;
		LOADING_SCREEN();
		nutchSrv.getAvailableNodes(vm.currentCluster.clusterName).success(function (data) {
			vm.availableNodes = data;
			LOADING_SCREEN('none');
		});
		ngDialog.open({
			template: 'plugins/nutch/partials/addNodesForm.html',
			scope: $scope
		});
	}

	function addNode(chosenNode) {
		if(chosenNode === undefined) return;
		if(vm.currentCluster.clusterName === undefined) return;
		LOADING_SCREEN();
		SweetAlert.swal("Success!", "Adding node action started.", "success");
		ngDialog.closeAll();
		nutchSrv.addNode(vm.currentCluster.clusterName, chosenNode).success(function (data) {
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
		nutchSrv.getHadoopClusters(selectedCluster).success(function (data) {
			vm.currentClusterNodes = data.slaves;
			vm.currentClusterNodes.push(data.nameNode);
			LOADING_SCREEN('none');
			console.log(vm.currentClusterNodes);
		});
	}

	function createNutch() {
		if(vm.nutchInstall.clusterName === undefined || vm.nutchInstall.clusterName.length == 0) return;
		if(vm.nutchInstall.hadoopClusterName === undefined || vm.nutchInstall.hadoopClusterName.length == 0) return;

		SweetAlert.swal("Success!", "Nutch cluster start creating.", "success");
		LOADING_SCREEN();
		nutchSrv.createNutch(vm.nutchInstall).success(function (data) {
			SweetAlert.swal("Success!", "Your Nutch cluster was created.", "success");
			LOADING_SCREEN("none");
			getClusters();
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Nutch cluster create error: ' + error.replace(/\\n/g, ' '), "error");
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
				nutchSrv.deleteCluster(vm.currentCluster.clusterName).success(function (data) {
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
		console.log (nodeId);
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
				nutchSrv.deleteNode(vm.currentCluster.clusterName, nodeId).success(function (data) {
					SweetAlert.swal("Deleted!", "Node has been deleted.", "success");
					getClustersInfo(vm.currentCluster.clusterName);
				});
			}
		});
	}

	function addContainer(containerId) {
		if(vm.nutchInstall.nodes.indexOf(containerId) > -1) {
			vm.nutchInstall.nodes.splice(vm.nutchInstall.nodes.indexOf(containerId), 1);
		} else {
			vm.nutchInstall.nodes.push(containerId);
		}
	}

	function setDefaultValues() {
		vm.nutchInstall = {};
		vm.nutchInstall.nodes = [];
	}

	vm.info = {};
    nutchSrv.getPluginInfo().success (function (data) {
    	vm.info = data;
    });
}

function colSelectNutchNodes() {
	return {
		restrict: 'E',
		templateUrl: 'plugins/nutch/directives/col-select/col-select-containers.html'
	}
};

