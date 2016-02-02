'use strict';

angular.module('subutai.plugins.flume.controller', [])
    .controller('FlumeCtrl', FlumeCtrl)
	.directive('colSelectFlumeNodes', colSelectFlumeNodes);

FlumeCtrl.$inject = ['$scope', 'flumeSrv', 'SweetAlert', 'DTOptionsBuilder', 'DTColumnDefBuilder', 'ngDialog'];

function FlumeCtrl($scope, flumeSrv, SweetAlert, DTOptionsBuilder, DTColumnDefBuilder, ngDialog) {
    var vm = this;
	vm.activeTab = 'install';
	vm.flumeInstall = {};
	vm.clusters = [];
	vm.containers = [];
	vm.currentCluster = [];
	vm.availableNodes = [];
	vm.selectedCluster = '';
	vm.nodes2Action = [];
	vm.currentClusterNodes = [];
	vm.hadoopClusters = [];

	//functions
	vm.getClustersInfo = getClustersInfo;	
	vm.addContainer = addContainer;	
	vm.createFlume = createFlume;	
	vm.deleteNode = deleteNode;
	vm.addNodeForm = addNodeForm;
	vm.addNode = addNode;
	vm.pushNode = pushNode;
	vm.pushAll = pushAll;
	vm.deleteCluster = deleteCluster;
	vm.startNodes = startNodes;
	vm.stopNodes = stopNodes;
	vm.getHadoopClusterNodes = getHadoopClusterNodes;

	flumeSrv.getHadoopClusters().success(function(data){
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
		flumeSrv.getClusters().success(function (data) {
			vm.clusters = data;
		});
	}
	getClusters();

	function getHadoopClusterNodes(selectedCluster) {
		LOADING_SCREEN();
		flumeSrv.getHadoopClusters(selectedCluster).success(function (data) {
			vm.currentClusterNodes = data.dataNodes;
			var tempArray = [];

			var nameNodeFound = false;
			var jobTrackerFound = false;
			var secondaryNameNodeFound = false;
			for(var i = 0; i < vm.currentClusterNodes.length; i++) {
				var node = vm.currentClusterNodes[i];
				if(node.hostname === data.nameNode.hostname) nameNodeFound = true;
				if(node.hostname === data.jobTracker.hostname) jobTrackerFound = true;
				if(node.hostname === data.secondaryNameNode.hostname) secondaryNameNodeFound = true;
			}
			if(!nameNodeFound) {
				tempArray.push(data.nameNode);
			}
			if(!jobTrackerFound) {
				if(tempArray[0].hostname != data.jobTracker.hostname) {
					tempArray.push(data.jobTracker);
				}
			}
			if(!secondaryNameNodeFound) {
				var checker = 0;
				for(var i = 0; i < tempArray.length; i++) {
					if(tempArray[i].hostname != data.secondaryNameNode.hostname) {
						checker++;
					}
				}
				if(checker == tempArray.length) {
					tempArray.push(data.secondaryNameNode);
				}
			}
			vm.currentClusterNodes = vm.currentClusterNodes.concat(tempArray);

			LOADING_SCREEN('none');
			console.log(vm.currentClusterNodes);
		});
	}

	vm.dtOptions = DTOptionsBuilder
		.newOptions()
		.withOption('order', [[2, "asc" ]])
		.withOption('stateSave', true)
		.withPaginationType('full_numbers');

	vm.dtColumnDefs = [
		DTColumnDefBuilder.newColumnDef(0).notSortable(),
		DTColumnDefBuilder.newColumnDef(1),
		DTColumnDefBuilder.newColumnDef(2),
		DTColumnDefBuilder.newColumnDef(3),
		DTColumnDefBuilder.newColumnDef(4).notSortable()
	];

	function getClustersInfo(selectedCluster) {
		LOADING_SCREEN();
		flumeSrv.getClusters(selectedCluster).success(function (data) {
			vm.currentCluster = data;
			LOADING_SCREEN('none');
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Cluster get info error: ' + error.replace(/\\n/g, ' '), "error");
			LOADING_SCREEN('none');
		});
	}

	function startNodes() {
		if(vm.nodes2Action.length == 0) return;
		if(vm.currentCluster.clusterName === undefined) return;
		SweetAlert.swal({
			title : 'Success!',
			text : 'Your request is in progress. You will be notified shortly.',
			timer: VARS_TOOLTIP_TIMEOUT,
			showConfirmButton: false
		});
		flumeSrv.startNodes(vm.currentCluster.clusterName, JSON.stringify(vm.nodes2Action)).success(function (data) {
			SweetAlert.swal("Success!", "Your cluster nodes started successfully.", "success");
			getClustersInfo(vm.currentCluster.clusterName);
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Cluster start error: ' + error.replace(/\\n/g, ' '), "error");
		});
	}

	function stopNodes() {
		if(vm.nodes2Action.length == 0) return;
		if(vm.currentCluster.clusterName === undefined) return;
		SweetAlert.swal({
			title : 'Success!',
			text : 'Your request is in progress. You will be notified shortly.',
			timer: VARS_TOOLTIP_TIMEOUT,
			showConfirmButton: false
		});
		flumeSrv.stopNodes(vm.currentCluster.clusterName, JSON.stringify(vm.nodes2Action)).success(function (data) {
			SweetAlert.swal("Success!", "Your cluster nodes stoped successfully.", "success");
			getClustersInfo(vm.currentCluster.clusterName);
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Cluster stop error: ' + error.replace(/\\n/g, ' '), "error");
		});
	}

	function pushNode(id) {
		if(vm.nodes2Action.indexOf(id) >= 0) {
			vm.nodes2Action.splice(vm.nodes2Action.indexOf(id), 1);
		} else {
			vm.nodes2Action.push(id);
		}
	}

	function pushAll() {
		if (vm.currentCluster.containers !== undefined) {
			if (vm.nodes2Action.length === vm.currentCluster.containers.length) {
				vm.nodes2Action = [];
			}
			else {
				for (var i = 0; i < vm.currentCluster.containers.length; ++i) {
					vm.nodes2Action.push(vm.currentCluster.containers[i].hostname);
				}
			}
		}
	}

	function addNodeForm() {
		if(vm.currentCluster.clusterName === undefined) return;
		flumeSrv.getAvailableNodes(vm.currentCluster.clusterName).success(function (data) {
			vm.availableNodes = data;
			console.log(vm.availableNodes);
		});
		ngDialog.open({
			template: 'plugins/flume/partials/addNodesForm.html',
			scope: $scope
		});
	}

	function addNode(chosenNode) {
		if(chosenNode === undefined) return;
		if(vm.currentCluster.clusterName === undefined) return;
		SweetAlert.swal("Success!", "Adding node action started.", "success");
		ngDialog.closeAll();
		flumeSrv.addNode(vm.currentCluster.clusterName, chosenNode).success(function (data) {
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

	function createFlume() {
		if(vm.flumeInstall.clusterName === undefined || vm.flumeInstall.clusterName.length == 0) return;

		SweetAlert.swal("Success!", "Elastic Search cluster start creating.", "success");
		flumeSrv.createFlume(vm.flumeInstall).success(function (data) {
			SweetAlert.swal("Success!", "Your Elastic Search cluster created.", "success");
			getClusters();
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Elastic Search cluster create error: ' + error.replace(/\\n/g, ' '), "error");
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
				flumeSrv.deleteCluster(vm.currentCluster.clusterName).success(function (data) {
					SweetAlert.swal("Deleted!", "Cluster has been deleted.", "success");
					vm.currentCluster = {};
					getClusters();
				}).error(function(error){
					SweetAlert.swal("ERROR!", 'Delete cluster error: ' + error.replace(/\\n/g, ' '), "error");
					vm.currentCluster = {};
					getClusters();
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
				flumeSrv.deleteNode(vm.currentCluster.clusterName, nodeId).success(function (data) {
					SweetAlert.swal("Deleted!", "Node has been deleted.", "success");
					getClustersInfo(vm.currentCluster.clusterName);
				});
			}
		});
	}

	function addContainer(containerId) {
		if(vm.flumeInstall.nodes.indexOf(containerId) > -1) {
			vm.flumeInstall.nodes.splice(vm.flumeInstall.nodes.indexOf(containerId), 1);
		} else {
			vm.flumeInstall.nodes.push(containerId);
		}
	}

	function setDefaultValues() {
		vm.flumeInstall = {};
		vm.flumeInstall.nodes = [];
	}


	vm.info = {};
	flumeSrv.getPluginInfo().success (function (data) {
		vm.info = data;
	});
}

function colSelectFlumeNodes() {
	return {
		restrict: 'E',
		templateUrl: 'plugins/flume/directives/col-select/col-select-containers.html'
	}
};

