'use strict';

angular.module('subutai.plugins.spark.controller', [])
    .controller('SparkCtrl', SparkCtrl)
    .directive('colSelectSparkNodes', colSelectSparkNodes);

SparkCtrl.$inject = ['$scope', 'sparkSrv', 'SweetAlert', 'DTOptionsBuilder', 'DTColumnDefBuilder', 'ngDialog'];

function SparkCtrl($scope, sparkSrv, SweetAlert, DTOptionsBuilder, DTColumnDefBuilder, ngDialog) {
    var vm = this;
    vm.activeTab = 'install';
    vm.sparkAll = false;
    vm.clients = [];
    vm.sparkInstall = {};
    vm.clusters = [];
    vm.hadoopClusters = [];
    vm.currentClusterNodes = [];
    vm.currentCluster = {};
    vm.availableNodes = [];
    vm.otherNodes = [];
    vm.nodes2Action = [];
    vm.master = {};


    //functions
    vm.getClustersInfo = getClustersInfo;
    vm.getHadoopClusterNodes = getHadoopClusterNodes;
    vm.addContainer = addContainer;
    vm.createSpark = createSpark;
    vm.deleteNode = deleteNode;
    vm.addNodeForm = addNodeForm;
    vm.addNode = addNode;
    vm.deleteCluster = deleteCluster;
    vm.changeDirective = changeDirective;
    vm.startMaster = startMaster;
    vm.stopMaster = stopMaster;
    vm.pushNode = pushNode;
    vm.pushAll = pushAll;
    vm.startNodes = startNodes;
    vm.stopNodes = stopNodes;

    sparkSrv.getHadoopClusters().success(function (data) {
        console.log(data);
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
        sparkSrv.getClusters().success(function (data) {
            vm.clusters = data;
            LOADING_SCREEN("none");
        });
    }

    getClusters();

    function pushNode(id) {
        if (vm.nodes2Action.indexOf(id) >= 0) {
            vm.nodes2Action.splice(vm.nodes2Action.indexOf(id), 1);
            vm.sparkAll = false;
        } else {
            vm.nodes2Action.push(id);
            if (vm.nodes2Action.length === vm.currentCluster.clients.length) {
                vm.sparkAll = true;
            }
        }
    }

    function pushAll() {
        if (vm.currentCluster.clients !== undefined) {
            if (vm.nodes2Action.length === vm.currentCluster.clients.length) {
                vm.nodes2Action = [];
                vm.sparkAll = false;
                for (var i = 0; i < vm.currentCluster.clients.length; ++i) {
                    vm.currentCluster.clients[i].checkbox = false;
                }
            }
            else {
                for (var i = 0; i < vm.currentCluster.clients.length; ++i) {
                    vm.nodes2Action.push(vm.currentCluster.clients[i].uuid);
                    vm.currentCluster.clients[i].checkbox = true;
                }
                vm.sparkAll = true;
            }
        }
    }


    function startNodes() {
        if (vm.nodes2Action.length == 0) return;
        if (vm.currentCluster.clusterName === undefined) return;
        SweetAlert.swal({
            title: 'Success!',
            text: 'Your request is in progress. You will be notified shortly.',
            timer: VARS_TOOLTIP_TIMEOUT,
            showConfirmButton: false
        });
        sparkSrv.startNodes(vm.currentCluster.clusterName, JSON.stringify(vm.nodes2Action)).success(function (data) {
            SweetAlert.swal("Success!", "Your cluster slaves started successfully.", "success");
            getClustersInfo(vm.currentCluster.clusterName);
            vm.nodes2Action = [];
            vm.sparkAll = false;
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Cluster slaves start error: ' + error.replace(/\\n/g, ' '), "error");
        });
    }

    function stopNodes() {
        if (vm.nodes2Action.length == 0) return;
        if (vm.currentCluster.clusterName === undefined) return;
        SweetAlert.swal({
            title: 'Success!',
            text: 'Your request is in progress. You will be notified shortly.',
            timer: VARS_TOOLTIP_TIMEOUT,
            showConfirmButton: false
        });
        sparkSrv.stopNodes(vm.currentCluster.clusterName, JSON.stringify(vm.nodes2Action)).success(function (data) {
            SweetAlert.swal("Success!", "Your cluster slaves have stopped successfully.", "success");
            getClustersInfo(vm.currentCluster.clusterName);
            vm.nodes2Action = [];
            vm.sparkAll = false;
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Failed to stop cluster slaves error: ' + error.replace(/\\n/g, ' '), "error");
        });
    }

    vm.dtOptions = DTOptionsBuilder
        .newOptions()
        .withOption('order', [[2, "asc"]])
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
        sparkSrv.getClusters(selectedCluster).success(function (data) {
            vm.currentCluster = data;
            for (var i = 0; i < vm.currentCluster.clients.length; ++i) {
                vm.currentCluster.clients[i].checkbox = false;
            }
            LOADING_SCREEN('none');
        });
    }

    function addNodeForm() {

        if (vm.currentCluster.environmentDataSource == "hub") {
            SweetAlert.swal("Feature coming soon...", "This environment created on Hub. Please use Hub to manage it.", "success");

            return;
        }

        if (vm.currentCluster.clusterName === undefined) return;
        sparkSrv.getAvailableNodes(vm.currentCluster.clusterName).success(function (data) {
            vm.availableNodes = data;
        });
        ngDialog.open({
            template: 'plugins/spark/partials/addNodesForm.html',
            scope: $scope
        });
    }

    function addNode(chosenNode) {
        if (chosenNode === undefined) return;
        if (vm.currentCluster.clusterName === undefined) return;
        LOADING_SCREEN();
        SweetAlert.swal("Success!", "Node is being added.", "success");
        ngDialog.closeAll();
        sparkSrv.addNode(vm.currentCluster.clusterName, chosenNode).success(function (data) {
            SweetAlert.swal(
                "Success!",
                "Node has been added to cluster " + vm.currentCluster.clusterName + ".",
                "success"
            );
            getClustersInfo(vm.currentCluster.clusterName);
            LOADING_SCREEN('none');
        });
    }

    function getHadoopClusterNodes(selectedCluster) {
        LOADING_SCREEN();
        sparkSrv.getHadoopClusters(selectedCluster).success(function (data) {
            vm.currentClusterNodes = data.slaves;
            vm.master = data.nameNode;
            LOADING_SCREEN('none');
        });
    }

    function createSpark() {
        if (vm.sparkInstall.hadoopClusterName === undefined) {
            SweetAlert.swal("ERROR!", "Please select Hadoop cluster", "error");
        }
        else if (vm.sparkInstall.nodes.length == 0) {
            SweetAlert.swal("ERROR!", "Please set nodes", "error");
        }
        else {
            SweetAlert.swal("Success!", "Spark cluster is being created.", "success");
            LOADING_SCREEN();
            sparkSrv.createSpark(vm.sparkInstall).success(function (data) {
                SweetAlert.swal("Success!", "Your Spark cluster was created.", "success");
                LOADING_SCREEN("none");
                getClusters();
                vm.activeTab = 'manage';
            }).error(function (error) {
                SweetAlert.swal("ERROR!", 'Spark cluster creation error: ' + error.replace(/\\n/g, ' '), "error");
                LOADING_SCREEN("none");
                getClusters();
            });
            setDefaultValues();
        }
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
                    sparkSrv.deleteCluster(vm.currentCluster.clusterName).success(function (data) {
                        SweetAlert.swal("Deleted!", "Cluster has been deleted.", "success");
                        vm.currentCluster = {};
                        getClusters();
                    }).error(function (error) {
                        SweetAlert.swal("ERROR!", 'Failed to delete cluster error: ' + error.replace(/\\n/g, ' '), "error");
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
                    sparkSrv.deleteNode(vm.currentCluster.clusterName, nodeId).success(function (data) {
                        SweetAlert.swal("Deleted!", "Node has been deleted.", "success");
                        getClustersInfo(vm.currentCluster.clusterName);
                    }).error(function (error) {
                        SweetAlert.swal("ERROR!", 'Failed to delete cluster node error: ' + error.replace(/\\n/g, ' '), "error");
                    });
                }
            });
    }

    function addContainer(containerId) {
        if (vm.sparkInstall.nodes.indexOf(containerId) > -1) {
            vm.sparkInstall.nodes.splice(vm.sparkInstall.nodes.indexOf(containerId), 1);
        } else {
            vm.sparkInstall.nodes.push(containerId);
        }
    }

    function setDefaultValues() {
        vm.sparkInstall = {};
        vm.sparkInstall.nodes = [];
        vm.sparkInstall.server = {};
        vm.otherNodes = [];
    }


    function changeDirective(server) {
        vm.otherNodes = [];
        for (var i = 0; i < vm.currentClusterNodes.length; ++i) {
            if (vm.currentClusterNodes[i].uuid !== server) {
                vm.otherNodes.push(vm.currentClusterNodes[i]);
            }
        }
    }

    function startMaster() {
        if (vm.currentCluster.clusterName === undefined) return;
        vm.currentCluster.server.status = 'STARTING';
        sparkSrv.startMasterNode(vm.currentCluster.clusterName, vm.currentCluster.server.uuid).success(function (data) {
            SweetAlert.swal("Success!", "Your server has been started.", "success");
            vm.currentCluster.server.status = 'RUNNING';
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Failed to start Spark server error: ' + error.replace(/\\n/g, ' '), "error");
            vm.currentCluster.server.status = 'ERROR';
        });
    }


    function stopMaster() {
        if (vm.currentCluster.clusterName === undefined) return;
        vm.currentCluster.server.status = 'STOPPING';
        sparkSrv.stopMasterNode(vm.currentCluster.clusterName, vm.currentCluster.server.uuid).success(function (data) {
            SweetAlert.swal("Success!", "Your server has been stopped.", "success");
            vm.currentCluster.server.status = 'STOPPED';
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Failed to stop Spark server error: ' + error.replace(/\\n/g, ' '), "error");
            vm.currentCluster.server.status = 'ERROR';
        });
    }

    vm.info = {};
    sparkSrv.getPluginInfo().success(function (data) {
        vm.info = data;
    });
}

function colSelectSparkNodes() {
    return {
        restrict: 'E',
        templateUrl: 'plugins/spark/directives/col-select/col-select-containers.html'
    }
};

