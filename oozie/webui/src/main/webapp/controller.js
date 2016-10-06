'use strict';

angular.module('subutai.plugins.oozie.controller', [])
    .controller('OozieCtrl', OozieCtrl)
	.directive('colSelectOozieNodes', colSelectOozieNodes);

OozieCtrl.$inject = ['$scope', 'oozieSrv', 'SweetAlert', 'DTOptionsBuilder', 'DTColumnDefBuilder', 'ngDialog'];

function OozieCtrl($scope, oozieSrv, SweetAlert, DTOptionsBuilder, DTColumnDefBuilder, ngDialog) {
    var vm = this;
	vm.activeTab = 'install';
	vm.oozieInstall = {};
	vm.clusters = [];
	vm.hadoopClusters = [];
	vm.currentClusterNodes = [];
	vm.currentCluster = {};
	vm.availableNodes = [];
	vm.otherNodes = [];
	vm.temp = [];
	vm.master = {};


	//functions
	vm.getClustersInfo = getClustersInfo;	
	vm.getHadoopClusterNodes = getHadoopClusterNodes;	
	vm.addContainer = addContainer;	
	vm.createOozie = createOozie;
	vm.deleteNode = deleteNode;
	vm.addNodeForm = addNodeForm;
	vm.addNode = addNode;
	vm.deleteCluster = deleteCluster;
	vm.changeDirective = changeDirective;
	vm.startServer = startServer;
	vm.stopServer = stopServer;

	oozieSrv.getHadoopClusters().success(function(data){
		vm.hadoopClusters = data;
		if(vm.hadoopClusters.length == 0) {
			SweetAlert.swal("ERROR!", 'No Hadoop clusters was found! Create Hadoop cluster first.', "error");
		}
	}).error(function(error){
		SweetAlert.swal("ERROR!", 'No Hadoop clusters was found! ERROR: ' + error.replace(/\\n/g, ' '), "error");
	});
	setDefaultValues();

	function getClusters() {
	    LOADING_SCREEN();
		oozieSrv.getClusters().success(function (data) {
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
		oozieSrv.getClusters(selectedCluster).success(function (data) {
			vm.currentCluster = data;
			LOADING_SCREEN('none');
		});
	}

	function addNodeForm() {
		if(vm.currentCluster.clusterName === undefined) return;
		oozieSrv.getAvailableNodes(vm.currentCluster.clusterName).success(function (data) {
			vm.availableNodes = data;
		});
		ngDialog.open({
			template: 'plugins/oozie/partials/addNodesForm.html',
			scope: $scope
		});
	}

	function addNode(chosenNode) {
		if(chosenNode === undefined) return;
		if(vm.currentCluster.clusterName === undefined) return;
		SweetAlert.swal("Success!", "Adding node action started.", "success");
		ngDialog.closeAll();
		oozieSrv.addNode(vm.currentCluster.clusterName, chosenNode).success(function (data) {
			SweetAlert.swal(
				"Success!",
				"Node has been added on cluster " + vm.currentCluster.clusterName + ".",
				"success"
			);
			getClustersInfo(vm.currentCluster.clusterName);
		});
	}

	function getHadoopClusterNodes(selectedCluster) {
		LOADING_SCREEN();
		oozieSrv.getHadoopClusters(selectedCluster).success(function (data) {
			vm.currentClusterNodes = data.slaves;
			vm.master = data.nameNode;
			LOADING_SCREEN('none');
		});
	}

	function createOozie() {
		if(vm.oozieInstall.clusterName === undefined || vm.oozieInstall.clusterName.length == 0) return;
		if(vm.oozieInstall.hadoopClusterName === undefined || vm.oozieInstall.hadoopClusterName.length == 0) return;
		SweetAlert.swal("Success!", "Oozie cluster start creating.", "success");
		LOADING_SCREEN();
		oozieSrv.createOozie(vm.oozieInstall).success(function (data) {
			SweetAlert.swal("Success!", "Your Oozie cluster successfully created.", "success");
			LOADING_SCREEN("none");
			getClusters();
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Oozie cluster create error: ' + error.replace(/\\n/g, ' '), "error");
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
				oozieSrv.deleteCluster(vm.currentCluster.clusterName).success(function (data) {
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
				oozieSrv.deleteNode(vm.currentCluster.clusterName, nodeId).success(function (data) {
					SweetAlert.swal("Deleted!", "Node has been deleted.", "success");
					getClustersInfo(vm.currentCluster.clusterName);
				});
			}
		});
	}

	function addContainer(containerId) {
		if(vm.oozieInstall.nodes.indexOf(containerId) > -1) {
			vm.oozieInstall.nodes.splice(vm.oozieInstall.nodes.indexOf(containerId), 1);
		} else {
			vm.oozieInstall.nodes.push(containerId);
		}
	}

	function setDefaultValues() {
		vm.oozieInstall = {};
		vm.oozieInstall.nodes = [];
		vm.oozieInstall.server = {};
	}


	function changeDirective(server) {
		vm.otherNodes = [];
		for (var i = 0; i < vm.currentClusterNodes.length; ++i) {
			if (vm.currentClusterNodes[i].uuid !== server) {
				vm.otherNodes.push (vm.currentClusterNodes[i]);
			}
		}
	}

	function startServer() {
		if(vm.currentCluster.clusterName === undefined) return;
		vm.currentCluster.server.status = 'STARTING';
		oozieSrv.startNode (vm.currentCluster.clusterName, vm.currentCluster.server.hostname).success (function (data) {
			SweetAlert.swal("Success!", "Your server started.", "success");
			vm.currentCluster.server.status = 'RUNNING';
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Oozie server start error: ' + error.replace(/\\n/g, ' '), "error");
			vm.currentCluster.server.status = 'ERROR';
		});
	}


	function stopServer() {
		if(vm.currentCluster.clusterName === undefined) return;
		vm.currentCluster.server.status = 'STOPPING';
		oozieSrv.stopNode (vm.currentCluster.clusterName, vm.currentCluster.server.hostname).success (function (data) {
			SweetAlert.swal("Success!", "Your server stopped.", "success");
			vm.currentCluster.server.status = 'STOPPED';
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Oozie server stop error: ' + error.replace(/\\n/g, ' '), "error");
			vm.currentCluster.server.status = 'ERROR';
		});
	}


	vm.info = {};
    oozieSrv.getPluginInfo().success (function (data) {
    	vm.info = data;
    });
}

function colSelectOozieNodes() {
	return {
		restrict: 'E',
		templateUrl: 'plugins/oozie/directives/col-select/col-select-containers.html'
	}
};

