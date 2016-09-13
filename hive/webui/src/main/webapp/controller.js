'use strict';

angular.module('subutai.plugins.hive.controller', [])
    .controller('HiveCtrl', HiveCtrl)
    .directive('colSelectHiveNodes', colSelectHiveNodes);

HiveCtrl.$inject = ['$scope', 'hiveSrv', 'SweetAlert', 'DTOptionsBuilder', 'DTColumnDefBuilder', 'ngDialog'];

function HiveCtrl($scope, hiveSrv, SweetAlert, DTOptionsBuilder, DTColumnDefBuilder, ngDialog) {
    var vm = this;
    vm.activeTab = 'install';
    vm.hiveInstall = {};
    vm.clusters = [];
    vm.hadoopClusters = [];
    vm.currentClusterNodes = [];
    vm.currentCluster = {};
    vm.availableNodes = [];
    vm.otherNodes = [];
    vm.temp = [];
    vm.otherNodes = [];

    //functions
    vm.getClustersInfo = getClustersInfo;
    vm.getHadoopClusterNodes = getHadoopClusterNodes;
    vm.createHive = createHive;
    vm.deleteNode = deleteNode;
    vm.addNodeForm = addNodeForm;
    vm.addNode = addNode;
    vm.deleteCluster = deleteCluster;
    vm.startServer = startServer;
    vm.stopServer = stopServer;
    vm.changeDirective = changeDirective;
    vm.addContainer = addContainer;
    vm.addAllContainers = addAllContainers;
    vm.unselectAllContainers = unselectAllContainers;


    hiveSrv.getHadoopClusters().success(function (data) {
        vm.hadoopClusters = data;
        if (vm.hadoopClusters.length == 0) {
            SweetAlert.swal("ERROR!", 'No Hadoop clusters were found! Create Hadoop cluster first.', "error");
        }
    }).error(function (error) {
        SweetAlert.swal("ERROR!", 'No Hadoop clusters were found! ERROR: ' + error.replace(/\\n/g, ' '), "error");
    });
    setDefaultValues();

    function getClusters() {
        LOADING_SCREEN();
        hiveSrv.getClusters().success(function (data) {
            vm.clusters = data;
            LOADING_SCREEN("none");
        });
    }

    getClusters();

    vm.dtOptions = DTOptionsBuilder
        .newOptions()
        .withOption('order', [[0, "asc"]])
        .withOption('stateSave', true);

    vm.dtColumnDefs = [
        DTColumnDefBuilder.newColumnDef(0),
        DTColumnDefBuilder.newColumnDef(1),
        DTColumnDefBuilder.newColumnDef(2).notSortable()
    ];

    function getClustersInfo(selectedCluster) {
        LOADING_SCREEN();
        hiveSrv.getClusters(selectedCluster).success(function (data) {
            vm.temp = [1];
            vm.currentCluster = data;
            LOADING_SCREEN('none');
        });
    }

    function addNodeForm() {
        if (vm.currentCluster.clusterName === undefined) return;
        hiveSrv.getAvailableNodes(vm.currentCluster.clusterName).success(function (data) {
            vm.availableNodes = data;
        });
        ngDialog.open({
            template: 'plugins/hive/partials/addNodesForm.html',
            scope: $scope
        });
    }

    function addNode(chosenNode) {
        if (chosenNode === undefined) return;
        if (vm.currentCluster.clusterName === undefined) return;
        LOADING_SCREEN();
        SweetAlert.swal("Success!", "Node adding is being started.", "success");
        ngDialog.closeAll();
        hiveSrv.addNode(vm.currentCluster.clusterName, chosenNode).success(function (data) {
            SweetAlert.swal(
                "Success!",
                "Node has been added to cluster " + vm.currentCluster.clusterName + ".",
                "success"
            );
            getClustersInfo(vm.currentCluster.clusterName);
            LOADING_SCREEN('none');
        });
    }

    function changeDirective(server) {
        vm.otherNodes = [];
        for (var i = 0; i < vm.currentClusterNodes.length; ++i) {
            if (vm.currentClusterNodes[i].uuid !== server) {
                vm.otherNodes.push(vm.currentClusterNodes[i]);
                // vm.hiveInstall.clients.push(vm.currentClusterNodes[i].uuid);
            }
        }
    }

    function addContainer(containerId) {
        if (vm.hiveInstall.clients.indexOf(containerId) > -1) {
            vm.hiveInstall.clients.splice(vm.hiveInstall.clients.indexOf(containerId), 1);
        } else {
            vm.hiveInstall.clients.push(containerId);
        }

        console.log(vm.hiveInstall.clients);
    }

    function addAllContainers() {
        vm.hiveInstall.clients = [];
        for (var i = 0; i < vm.containers.length; i++) {
            vm.hiveInstall.clients.push(vm.containers[i].id);
        }
    }

    function unselectAllContainers() {
        vm.hiveInstall.clients = [];
    }


    function getHadoopClusterNodes(selectedCluster) {
        LOADING_SCREEN();
        hiveSrv.getHadoopClusters(selectedCluster).success(function (data) {
            vm.currentClusterNodes = data.slaves;
            vm.currentClusterNodes.push(data.nameNode);
            vm.hiveInstall.namenode = data.nameNode;
            LOADING_SCREEN('none');
        }).error(function (error) {
            SweetAlert.swal("ERROR!", error.replace(/\\n/g, ' '), "error");
            LOADING_SCREEN('none');
        });
    }

    function createHive() {
        console.log(vm.hiveInstall);
        if (vm.hiveInstall.clusterName === undefined || vm.hiveInstall.clusterName.length == 0) return;
        if (vm.hiveInstall.hadoopClusterName === undefined || vm.hiveInstall.hadoopClusterName.length == 0) return;
        SweetAlert.swal("Success!", "Hive cluster is being created.", "success");
        LOADING_SCREEN();
        hiveSrv.createHive(vm.hiveInstall).success(function (data) {
            SweetAlert.swal("Success!", "Your Hive cluster is being created.", "success");
            LOADING_SCREEN("none");
            getClusters();
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Hive cluster creation error: ' + error.replace(/\\n/g, ' '), "error");
            LOADING_SCREEN("none");
        });
        setDefaultValues();
        vm.activeTab = 'manage';
    }

    function deleteCluster() {
        if (vm.currentCluster.clusterName === undefined) return;
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
                    hiveSrv.deleteCluster(vm.currentCluster.clusterName).success(function (data) {
                        SweetAlert.swal("Deleted!", "Cluster has been deleted.", "success");
                        vm.currentCluster = {};
                        getClusters();
                    }).error(function (error) {
                        SweetAlert.swal("ERROR!", 'Failed to delete cluster error: ' + data.replace(/\\n/g, ' '), "error");
                    });
                }
            });
    }

    function deleteNode(nodeId) {
        if (vm.currentCluster.clusterName === undefined) return;
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
                    hiveSrv.deleteNode(vm.currentCluster.clusterName, nodeId).success(function (data) {
                        SweetAlert.swal("Deleted!", "Node has been deleted.", "success");
                        getClustersInfo(vm.currentCluster.clusterName);
                    });
                }
            });
    }


    function setDefaultValues() {
        vm.hiveInstall = {};
        vm.hiveInstall.clients = [];
    }


    function startServer() {
        if (vm.currentCluster.clusterName === undefined) return;
        vm.currentCluster.server.status = 'STARTING';
        hiveSrv.startNode(vm.currentCluster.clusterName, vm.currentCluster.server.hostname).success(function (data) {
            SweetAlert.swal("Success!", "Your server has started.", "success");
            vm.currentCluster.server.status = 'RUNNING';
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Failed to start Hive server error: ' + error.replace(/\\n/g, ' '), "error");
            vm.currentCluster.server.status = 'ERROR';
        });
    }


    function stopServer() {
        if (vm.currentCluster.clusterName === undefined) return;
        vm.currentCluster.server.status = 'STOPPING';
        hiveSrv.stopNode(vm.currentCluster.clusterName, vm.currentCluster.server.hostname).success(function (data) {
            SweetAlert.swal("Success!", "Your server has stopped.", "success");
            vm.currentCluster.server.status = 'STOPPED';
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Hive server has stop error: ' + error.replace(/\\n/g, ' '), "error");
            vm.currentCluster.server.status = 'ERROR';
        });
    }

    vm.info = {};
    hiveSrv.getPluginInfo().success(function (data) {
        vm.info = data;
    });
}

function colSelectHiveNodes() {
    return {
        restrict: 'E',
        templateUrl: 'plugins/hive/directives/col-select/col-select-containers.html'
    }
};

