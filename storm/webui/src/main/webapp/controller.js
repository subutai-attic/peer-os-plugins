'use strict';

angular.module('subutai.plugins.storm.controller', [])
    .controller('StormCtrl', StormCtrl)
	.directive('colSelectStormNodes', colSelectStormNodes);

StormCtrl.$inject = ['$scope', 'stormSrv', 'SweetAlert', 'DTOptionsBuilder', 'DTColumnDefBuilder', 'ngDialog'];

function StormCtrl($scope, stormSrv, SweetAlert, DTOptionsBuilder, DTColumnDefBuilder, ngDialog) {
    var vm = this;
	vm.activeTab = 'install';
	vm.stormAll = false;
	vm.stormInstall = {};
	vm.clusters = [];
	vm.environments = [];
	vm.currentClusterNodes = [];
	vm.currentCluster = {};
	vm.availableNodes = [];
	vm.otherNodes = [];
	vm.nodes2Action = [];


	//functions
	vm.getClustersInfo = getClustersInfo;	
	vm.getEnvironmentNodes = getEnvironmentNodes;
	vm.addContainer = addContainer;	
	vm.createStorm = createStorm;
	vm.deleteNode = deleteNode;
	vm.addNode = addNode;
	vm.deleteCluster = deleteCluster;
	vm.changeDirective = changeDirective;
	vm.startNimbus = startNimbus;
	vm.stopNimbus = stopNimbus;
	vm.pushNode = pushNode;
	vm.pushAll = pushAll;
	vm.changeClusterScaling = changeClusterScaling;
	vm.startNodes = startNodes;
	vm.stopNodes = stopNodes;

	stormSrv.getEnvironments().success(function(data){
		vm.environments = data;
		if(vm.environments.length == 0) {
			SweetAlert.swal("ERROR!", 'No environments were found! Create environment first.', "error");
		}
	}).error(function(data){
		SweetAlert.swal("ERROR!", 'No environments were found! ERROR: ' + data, "error");
	});
	setDefaultValues();

	function getClusters() {
		vm.clusters = [];
		LOADING_SCREEN();
		stormSrv.getClusters().success(function (data) {
			console.log (data);
			vm.clusters = data;
			LOADING_SCREEN ("none");
		}).error (function (error) {;
			LOADING_SCREEN ("none");
		});
	}
	getClusters();

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
		DTColumnDefBuilder.newColumnDef(4),
		DTColumnDefBuilder.newColumnDef(5).notSortable(),
	];

	function getClustersInfo(selectedCluster) {
		LOADING_SCREEN();
		vm.currentCluster = {};
		stormSrv.getClusters(selectedCluster).success(function (data) {
			vm.currentCluster = data;
			for (var i = 0; i < vm.currentCluster.supervisors.length; ++i) {
			    vm.currentCluster.supervisors[i].checkbox = false;
			}
			LOADING_SCREEN('none');
		}).error (function (error) {
			LOADING_SCREEN ("none");
		});
	}

	function addNode() {
		if(vm.currentCluster.clusterName === undefined) return;
		LOADING_SCREEN();
		SweetAlert.swal("Success!", "node is being added.", "success");
		stormSrv.addNode(vm.currentCluster.clusterName).success(function (data) {
			SweetAlert.swal(
				"Success!",
				"Node has been added to cluster " + vm.currentCluster.clusterName + ".",
				"success"
			);
			getClustersInfo(vm.currentCluster.clusterName);
			LOADING_SCREEN('none');
		});
	}

	function getEnvironmentNodes(selectedCluster) {
		vm.currentClusterNodes = [];
		
		stormSrv.getContainers(selectedCluster).success(function (data) {
			vm.currentClusterNodes = data;
		});
	}

	function createStorm() {
		if(vm.stormInstall.clusterName === undefined || vm.stormInstall.clusterName.length == 0) return;
		if(vm.stormInstall.environmentId === undefined || vm.stormInstall.environmentId.length == 0) return;
		SweetAlert.swal("Success!", "Storm cluster is being created.", "success");
		LOADING_SCREEN();
		stormSrv.createStorm(vm.stormInstall).success(function (data) {
			SweetAlert.swal("Success!", "Your Storm cluster has been successfully created.", "success");
			getClusters();
			LOADING_SCREEN ("none");
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Storm cluster creation error: ' + error, "error");
			LOADING_SCREEN ("none");
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
				stormSrv.deleteCluster(vm.currentCluster.clusterName).success(function (data) {
					SweetAlert.swal("Deleted!", "Cluster has been deleted.", "success");
					vm.currentCluster = {};
					getClusters();
				}).error(function(data){
					SweetAlert.swal("ERROR!", 'Failed to delete cluster error: ' + data, "error");
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
				stormSrv.deleteNode(vm.currentCluster.clusterName, nodeId).success(function (data) {
					SweetAlert.swal("Deleted!", "Node has been deleted.", "success");
					getClustersInfo(vm.currentCluster.clusterName);
				});
			}
		});
	}

	function addContainer(containerId) {
		if(vm.stormInstall.nodes.indexOf(containerId) > -1) {
			vm.stormInstall.nodes.splice(vm.stormInstall.nodes.indexOf(containerId), 1);
		} else {
			vm.stormInstall.nodes.push(containerId);
		}
	}

	function setDefaultValues() {
		vm.stormInstall = {};
		vm.stormInstall.domainName = "intra.lan"
		vm.stormInstall.nodes = [];
		vm.stormInstall.nimbus = {};
	}


	function changeDirective(nimbus) {
		vm.otherNodes = [];
		for (var i = 0; i < vm.currentClusterNodes.length; ++i) {
			if (vm.currentClusterNodes[i].id !== nimbus) {
				vm.otherNodes.push (vm.currentClusterNodes[i]);
			}
		}
	}

	function startNimbus() {
		if(vm.currentCluster.clusterName === undefined) return;
		vm.currentCluster.nimbus.status = 'STARTING';
		stormSrv.startNode (vm.currentCluster.clusterName, vm.currentCluster.nimbus.id).success (function (data) {
			SweetAlert.swal("Success!", "Your nimbus has been started.", "success");
			vm.currentCluster.nimbus.status = 'RUNNING';
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Failed to startStorm nimbus error: ' + error, "error");
			vm.currentCluster.nimbus.status = 'ERROR';
		});
	}


	function stopNimbus() {
		if(vm.currentCluster.clusterName === undefined) return;
		vm.currentCluster.nimbus.status = 'STOPPING';
		stormSrv.stopNode (vm.currentCluster.clusterName, vm.currentCluster.nimbus.id).success (function (data) {
			SweetAlert.swal("Success!", "Your nimbus has been stopped.", "success");
			vm.currentCluster.nimbus.status = 'STOPPED';
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Failed to stop Storm nimbus error: ' + error, "error");
			vm.currentCluster.nimbus.status = 'ERROR';
		});
	}


	function checkIfPushed(id) {
		for (var i = 0; i < vm.nodes2Action.length; ++i) {
			if (vm.nodes2Action[i].name === id) {
				return i;
			}
		}
		return -1;
	}


	function pushNode(id, type) {
		var index = checkIfPushed (id);
		if(index !== -1) {
			vm.nodes2Action.splice(index, 1);
			vm.stormAll = false;
		} else {
			vm.nodes2Action.push({name: id, type: type});
			if (vm.nodes2Action.length === vm.currentCluster.supervisors.length + 1) {
                vm.stormAll = true;
            }
		}
	}


	function pushAll() {
        if (vm.nodes2Action.length === vm.currentCluster.supervisors.length + 1) {
            vm.nodes2Action = [];
            vm.stormAll = false;
            vm.currentCluster.nimbus.checkbox = false;
            for (var i = 0; i < vm.currentCluster.supervisors.length; ++i) {
                vm.currentCluster.supervisors[i].checkbox = false;
            }
        }
        else {
            vm.nodes2Action.push ({name: vm.currentCluster.nimbus.id, type: "nimbus"});
            vm.currentCluster.nimbus.checkbox = true;
            for (var i = 0; i < vm.currentCluster.supervisors.length; ++i) {
                vm.nodes2Action.push ({name: vm.currentCluster.supervisors[i].id, type: "supervisor"});
                vm.currentCluster.supervisors[i].checkbox = true;
            }
            vm.stormAll = true;
        }
	}


	function changeClusterScaling (val) {
		stormSrv.changeClusterScaling (vm.currentCluster.clusterName, val);
	}


	function startNodes() {
		if(vm.nodes2Action.length == 0) return;
		if(vm.currentCluster.clusterName === undefined) return;
		LOADING_SCREEN();
		SweetAlert.swal({
			title : 'Success!',
			text : 'Your request is in progress. You will be notified shortly.',
			timer: VARS_TOOLTIP_TIMEOUT,
			showConfirmButton: false
		});
		stormSrv.startNodes(vm.currentCluster.clusterName, JSON.stringify(vm.nodes2Action)).success(function (data) {
			SweetAlert.swal("Success!", "Your cluster nodes have been started successfully.", "success");
			getClustersInfo(vm.currentCluster.clusterName);
			vm.nodes2Action = [];
            vm.stormAll = false;
			LOADING_SCREEN ("none");
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Failed to start Cluster error: ' + error.ERROR, "error");
			LOADING_SCREEN ("none");
		});
	}


	function stopNodes() {
		if(vm.nodes2Action.length == 0) return;
		if(vm.currentCluster.clusterName === undefined) return;
		LOADING_SCREEN();
		SweetAlert.swal({
			title : 'Success!',
			text : 'Your request is in progress. You will be notified shortly.',
			timer: VARS_TOOLTIP_TIMEOUT,
			showConfirmButton: false
		});
		stormSrv.stopNodes(vm.currentCluster.clusterName, JSON.stringify(vm.nodes2Action)).success(function (data) {
			SweetAlert.swal("Success!", "Your cluster nodes have been stopped successfully.", "success");
			getClustersInfo(vm.currentCluster.clusterName);
			vm.nodes2Action = [];
            vm.stormAll = false;
			LOADING_SCREEN ("none");
		}).error(function (error) {
			SweetAlert.swal("ERROR!", 'Failed to stop cluster error: ' + error.ERROR, "error");
			LOADING_SCREEN ("none");
		});
	}

	vm.info = {};
    stormSrv.getPluginInfo().success (function (data) {
    	vm.info = data;
    });
}

function colSelectStormNodes() {
	return {
		restrict: 'E',
		templateUrl: 'plugins/storm/directives/col-select/col-select-containers.html'
	}
};

