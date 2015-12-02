'use strict';

angular.module('subutai.plugins.solr.controller', [])
    .controller('SolrCtrl', SolrCtrl)
	.directive('colSelectContainers', colSelectContainers);

SolrCtrl.$inject = ['solrSrv', 'SweetAlert'];
function SolrCtrl(solrSrv, SweetAlert) {
    var vm = this;
	vm.activeTab = 'install';
	vm.selectedOption = {};
	vm.solrInstall = {};
	vm.environments = [];
	vm.containers = [];

	vm.clusters = [];
	vm.currentCluster = {};
	vm.nodes2Action = [];

	//functions
	vm.showContainers = showContainers;
	vm.addContainer = addContainer;
	vm.createSolr = createSolr;

	vm.getClustersInfo = getClustersInfo;
	vm.changeClusterScaling = changeClusterScaling;
	vm.deleteCluster = deleteCluster;
	vm.addNode = addNode;
	vm.deleteNode = deleteNode;
	vm.pushNode = pushNode;
	vm.pushAll = pushAll;
	vm.startNodes = startNodes;
	vm.stopNodes = stopNodes;
	
	setDefaultValues();
	solrSrv.getEnvironments().success(function (data) {
		vm.environments = data;
	});
	function getClusters() {
		solrSrv.getClusters().success(function (data) {
			vm.clusters = data;
			//getClustersInfo(data[0]);
		});
	}
	getClusters();

	function getClustersInfo(selectedCluster) {
		LOADING_SCREEN();
		vm.nodes2Action = [];
		solrSrv.getClusters(selectedCluster).success(function (data){
			vm.currentCluster = data;
			LOADING_SCREEN('none');
		}).error(function(error){
			SweetAlert.swal("ERROR!", 'Cluster info error: ' + error.replace(/\\n/g, ' '), "error");
			LOADING_SCREEN('none');
		});
	}

	function startNodes() {
		if(vm.nodes2Action.length == 0) return;
		if(vm.currentCluster.name === undefined) return;
		vm.globalChecker = false;
		SweetAlert.swal({
			title : 'Success!',
			text : 'Your request is in progress. You will be notified shortly.',
			timer: VARS_TOOLTIP_TIMEOUT,
			showConfirmButton: false
		});
		solrSrv.startNodes(vm.currentCluster.name, JSON.stringify(vm.nodes2Action)).success(function (data) {
			SweetAlert.swal("Success!", "Your cluster nodes started successfully.", "success");
			getClustersInfo(vm.currentCluster.name);
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Cluster start error: ' + error.replace(/\\n/g, ' '), "error");
		});
	}

	function stopNodes() {
		if(vm.nodes2Action.length == 0) return;
		if(vm.currentCluster.name === undefined) return;
		vm.globalChecker = false;
		SweetAlert.swal({
			title : 'Success!',
			text : 'Your request is in progress. You will be notified shortly.',
			timer: VARS_TOOLTIP_TIMEOUT,
			showConfirmButton: false
		});
		solrSrv.stopNodes(vm.currentCluster.name, JSON.stringify(vm.nodes2Action)).success(function (data) {
			SweetAlert.swal("Success!", "Your cluster nodes stoped successfully.", "success");
			getClustersInfo(vm.currentCluster.name);
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

	function pushAll(check) {
		if (vm.currentCluster.containers !== undefined) {
			if (check) {
				for (var i = 0; i < vm.currentCluster.containers.length; ++i) {
					vm.nodes2Action.push(vm.currentCluster.containers[i].id);
				}
			}
			else {
				vm.nodes2Action = [];
			}
		}
	}

	function addNode() {
		if(vm.currentCluster.clusterName === undefined) return;
		SweetAlert.swal("Success!", "Adding node action started.", "success");
		solrSrv.addNode(vm.currentCluster.clusterName).success(function (data) {
			SweetAlert.swal(
				"Success!",
				"Node has been added on cluster " + vm.currentCluster.clusterName + ".",
				"success"
			);
			getClustersInfo(vm.currentCluster.clusterName);
		});
	}

	function deleteNode(nodeId) {
		if(vm.currentCluster.name === undefined) return;
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
				solrSrv.deleteNode(vm.currentCluster.name, nodeId).success(function (data) {
					SweetAlert.swal("Deleted!", "Node has been deleted.", "success");
					vm.currentCluster = {};
				});
			}
		});
	}

	function deleteCluster() {
		if(vm.currentCluster.name === undefined) return;
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
				solrSrv.deleteCluster(vm.currentCluster.name).success(function (data) {
					SweetAlert.swal("Deleted!", "Cluster has been deleted.", "success");
					vm.currentCluster = {};
					getClusters();
				});
			}
		});
	}
	
	function switchTab(tab) {
	
		if(tab == 'manager')
		{
			vm.activeTab = 'manage';
			getClusters();
		}
	}

	function createSolr() {
		SweetAlert.swal("Success!", "Solr cluster is being created.", "success");
		switchTab('manager');
		solrSrv.createSolr(vm.solrInstall).success(function (data) {
			SweetAlert.swal("Success!", "Solr cluster creation message:" + data.replace(/\\n/g, ' '), "success");
			getClusters();
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Solr cluster create error: ' + error.replace(/\\n/g, ' '), "error");
		});
	}

	function changeClusterScaling(scale) {
		if(vm.currentCluster.clusterName === undefined) return;
		try {
			solrSrv.changeClusterScaling(vm.currentCluster.clusterName, scale);
		} catch(e) {}
	}

	function showContainers(environmentId) {
		
		vm.containers = [];
		for(var i in vm.environments) {
			if(environmentId == vm.environments[i].id) {
				for (var j = 0; j < vm.environments[i].containers.length; j++){
					if(vm.environments[i].containers[j].templateName == 'solr') {
						vm.containers.push(vm.environments[i].containers[j]);
					}
				}
				break;
			}
		}
	}

	function addContainer(containerId) {
		if(vm.solrInstall.containers.indexOf(containerId) > -1) {
			vm.solrInstall.containers.splice(vm.solrInstall.containers.indexOf(containerId), 1);
		} else {
			vm.solrInstall.containers.push(containerId);
		}
	}
	
	function setDefaultValues() {
		vm.solrInstall.containers = [];
	}
}

function colSelectContainers() {
	return {
		restrict: 'E',
		templateUrl: 'plugins/solr/directives/col-select/col-select-containers.html'
	}
};


