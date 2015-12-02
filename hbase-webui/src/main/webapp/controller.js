'use strict';

angular.module('subutai.plugins.hbase.controller', [])
    .controller('HbaseCtrl', HbaseCtrl)
	.directive('colSelectRegionHbaseNodes', colSelectRegionHbaseNodes)
	.directive('colSelectQuorumHbaseNodes', colSelectQuorumHbaseNodes)
	.directive('colSelectBackupHbaseNodes', colSelectBackupHbaseNodes);

HbaseCtrl.$inject = ['$scope', 'hbaseSrv', 'SweetAlert', 'DTOptionsBuilder', 'DTColumnDefBuilder', 'ngDialog'];

function HbaseCtrl($scope, hbaseSrv, SweetAlert, DTOptionsBuilder, DTColumnDefBuilder, ngDialog) {
    var vm = this;
	vm.activeTab = 'install';
	vm.hbaseInstall = {};
	vm.clusters = [];
	vm.hadoopClusters = [];
	vm.currentClusterNodes = [];
	vm.currentCluster = [];
	vm.availableNodes = [];
	vm.otherNodes = [];
	vm.hadoopFullInfo = {};

	//functions
	vm.getClustersInfo = getClustersInfo;	
	vm.getHadoopClusterNodes = getHadoopClusterNodes;	
	vm.addContainer = addContainer;	
	vm.createHbase = createHbase;	
	vm.deleteNode = deleteNode;
	vm.addNodeForm = addNodeForm;
	vm.addNode = addNode;
	vm.deleteCluster = deleteCluster;
	vm.changeDirective = changeDirective;
	vm.changeClusterScaling = changeClusterScaling;
	vm.startNodes = startNodes;
	vm.stopNodes = stopNodes;

	hbaseSrv.getHadoopClusters().success(function(data){
		vm.hadoopClusters = data;
		if(vm.hadoopClusters.length == 0) {
			SweetAlert.swal("ERROR!", 'No Hadoop clusters was found! Create Hadoop cluster first.', "error");
		}
	}).error(function(error){
		SweetAlert.swal("ERROR!", 'No Hadoop clusters was found! ERROR: ' + error.replace(/\\n/g, ' '), "error");
	});
	setDefaultValues();

	function getClusters() {
		hbaseSrv.getClusters().success(function (data) {
			vm.clusters = data;
		});
	}
	getClusters();

	vm.dtOptions = DTOptionsBuilder
		.newOptions()
		.withOption('order', [[3, "asc" ]])
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
		hbaseSrv.getClusters(selectedCluster).success(function (data) {
			vm.currentCluster = data;
			LOADING_SCREEN('none');
		}).error(function(error){
			SweetAlert.swal("ERROR!", 'Get cluster info error: ' + error.replace(/\\n/g, ' '), "error");
			LOADING_SCREEN('none');
		});
	}

	function changeClusterScaling(scale) {
		if(vm.currentCluster.clusterName === undefined) return;
		try {
			hbaseSrv.changeClusterScaling(vm.currentCluster.clusterName, scale);
		} catch(e) {}
	}

	function startNodes() {
		if(vm.currentCluster.clusterName === undefined) return;
		SweetAlert.swal({
			title : 'Success!',
			text : 'Your request is in progress. You will be notified shortly.',
			timer: VARS_TOOLTIP_TIMEOUT,
			showConfirmButton: false
		});
		hbaseSrv.startNodes(vm.currentCluster.clusterName).success(function (data) {
			SweetAlert.swal("Success!", "Your cluster nodes have been started successfully.", "success");
			getClustersInfo(vm.currentCluster.clusterName);
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Cluster starting error: ' + error.replace(/\\n/g, ' '), "error");
		});
	}

	function stopNodes() {
		if(vm.currentCluster.clusterName === undefined) return;
		SweetAlert.swal({
			title : 'Success!',
			text : 'Your request is in progress. You will be notified shortly.',
			timer: VARS_TOOLTIP_TIMEOUT,
			showConfirmButton: false
		});
		hbaseSrv.stopNodes(vm.currentCluster.clusterName).success(function (data) {
			SweetAlert.swal("Success!", "Your cluster nodes have been stopped successfully.", "success");
			getClustersInfo(vm.currentCluster.clusterName);
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Failed to stop cluster error: ' + error.replace(/\\n/g, ' '), "error");
		});
	}

	function addNodeForm() {
		if(vm.currentCluster.clusterName === undefined) return;
		hbaseSrv.getAvailableNodes(vm.currentCluster.clusterName).success(function (data) {
			vm.availableNodes = data;
		});
		ngDialog.open({
			template: 'plugins/hbase/partials/addNodesForm.html',
			scope: $scope
		});
	}

	function addNode(chosenNode) {
		if(chosenNode === undefined) return;
		if(vm.currentCluster.clusterName === undefined) return;
		SweetAlert.swal("Success!", "Adding node action started.", "success");
		ngDialog.closeAll();
		hbaseSrv.addNode(vm.currentCluster.clusterName, chosenNode).success(function (data) {
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

	function changeDirective(hmaster) {
		vm.otherNodes = [];
		for (var i = 0; i < vm.currentClusterNodes.length; ++i) {
			if (vm.currentClusterNodes[i].uuid !== hmaster) {
				vm.otherNodes.push (vm.currentClusterNodes[i]);
			}
		}
	}

	function getHadoopClusterNodes(selectedCluster) {
		LOADING_SCREEN();
		hbaseSrv.getHadoopClusters(selectedCluster).success(function (data) {
			vm.hadoopFullInfo = data;
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
		});
	}

	function createHbase() {
		if(vm.hbaseInstall.clusterName === undefined || vm.hbaseInstall.clusterName.length == 0) return;
		if(vm.hbaseInstall.hadoopClusterName === undefined || vm.hbaseInstall.hadoopClusterName.length == 0) return;

		vm.hbaseInstall.environmentId = vm.hadoopFullInfo.environmentId;
		vm.hbaseInstall.domainName = 'intra.lan';

		SweetAlert.swal("Success!", "Hbase cluster start creating.", "success");
		hbaseSrv.createHbase(JSON.stringify(vm.hbaseInstall)).success(function (data) {
			SweetAlert.swal("Success!", "Your Hbase cluster start creating. LOG: " + data.replace(/\\n/g, ' '), "success");
			getClusters();
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Hbase cluster create error: ' + error.replace(/\\n/g, ' '), "error");
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
				hbaseSrv.deleteCluster(vm.currentCluster.clusterName).success(function (data) {
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
				hbaseSrv.deleteNode(vm.currentCluster.clusterName, nodeId).success(function (data) {
					SweetAlert.swal("Deleted!", "Node has been deleted. LOG: " + data.replace(/\\n/g, ' '), "success");
					getClustersInfo(vm.currentCluster.clusterName);
				}).error(function(error){
					SweetAlert.swal("ERROR!", 'Delete node error: ' + data.replace(/\\n/g, ' '), "error");
				});
			}
		});
	}

	function addContainer(containerId, field) {
		if(vm.hbaseInstall[field].indexOf(containerId) > -1) {
			vm.hbaseInstall[field].splice(vm.hbaseInstall[field].indexOf(containerId), 1);
		} else {
			vm.hbaseInstall[field].push(containerId);
		}
	}

	function setDefaultValues() {
		vm.hbaseInstall = {};
		vm.hbaseInstall.regionServers = [];
		vm.hbaseInstall.quorumPeers = [];
		vm.hbaseInstall.backupMasters = [];
		vm.hadoopFullInfo = {};
	}

}

function colSelectRegionHbaseNodes() {
	return {
		restrict: 'E',
		templateUrl: 'plugins/hbase/directives/col-select/col-select-regions.html'
	}
};

function colSelectQuorumHbaseNodes() {
	return {
		restrict: 'E',
		templateUrl: 'plugins/hbase/directives/col-select/col-select-quorum.html'
	}
};

function colSelectBackupHbaseNodes() {
	return {
		restrict: 'E',
		templateUrl: 'plugins/hbase/directives/col-select/col-select-backup.html'
	}
};

