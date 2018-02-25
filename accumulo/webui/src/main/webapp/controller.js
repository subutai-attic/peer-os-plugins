'use strict';

angular.module('subutai.plugins.accumulo.controller', [])
    .controller('AccumuloCtrl', AccumuloCtrl)
    .directive('colSelectAccumuloNodes', colSelectAccumuloNodes);

AccumuloCtrl.$inject = ['$scope', 'accumuloSrv', 'SweetAlert', 'DTOptionsBuilder', 'DTColumnDefBuilder', 'ngDialog'];

function AccumuloCtrl($scope, accumuloSrv, SweetAlert, DTOptionsBuilder, DTColumnDefBuilder, ngDialog) {
    var vm = this;
    vm.activeTab = 'install';
    vm.accumuloAll = false;
    vm.slaves = [];
    vm.accumuloInstall = {};
    vm.confirmPassword = "";
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
    vm.createAccumulo = createAccumulo;
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

    accumuloSrv.getHadoopClusters().success(function (data) {
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
        accumuloSrv.getClusters().success(function (data) {
            vm.clusters = data;
            LOADING_SCREEN("none");
        });
    }

    getClusters();

    function pushNode(id) {
        if (vm.nodes2Action.indexOf(id) >= 0) {
            vm.nodes2Action.splice(vm.nodes2Action.indexOf(id), 1);
            vm.accumuloAll = false;
        } else {
            vm.nodes2Action.push(id);
            if (vm.nodes2Action.length === vm.currentCluster.slaves.length) {
                vm.accumuloAll = true;
            }
        }
    }

    function pushAll() {
        if (vm.currentCluster.slaves !== undefined) {
            if (vm.nodes2Action.length === vm.currentCluster.slaves.length) {
                vm.nodes2Action = [];
                vm.accumuloAll = false;
                for (var i = 0; i < vm.currentCluster.slaves.length; ++i) {
                    vm.currentCluster.slaves[i].checkbox = false;
                }
            }
            else {
                for (var i = 0; i < vm.currentCluster.slaves.length; ++i) {
                    vm.nodes2Action.push(vm.currentCluster.slaves[i].id);
                    vm.currentCluster.slaves[i].checkbox = true;
                }
                vm.accumuloAll = true;
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
        accumuloSrv.startNodes(vm.currentCluster.clusterName, JSON.stringify(vm.nodes2Action)).success(function (data) {
            SweetAlert.swal("Success!", "Your cluster slaves started successfully.", "success");
            getClustersInfo(vm.currentCluster.clusterName);
            vm.nodes2Action = [];
            vm.accumuloAll = false;
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
        accumuloSrv.stopNodes(vm.currentCluster.clusterName, JSON.stringify(vm.nodes2Action)).success(function (data) {
            SweetAlert.swal("Success!", "Your cluster slaves have stopped successfully.", "success");
            getClustersInfo(vm.currentCluster.clusterName);
            vm.nodes2Action = [];
            vm.accumuloAll = false;
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
        accumuloSrv.getClusters(selectedCluster).success(function (data) {
            vm.currentCluster = data;
            for (var i = 0; i < vm.currentCluster.slaves.length; ++i) {
                vm.currentCluster.slaves[i].checkbox = false;
            }
            LOADING_SCREEN('none');
        });
    }

    function addNodeForm() {

        if (vm.currentCluster.environmentDataSource == "hub") {
            SweetAlert.swal("Feature coming soon...", "This environment created on Bazaar. Please use Bazaar to manage it.", "success");

            return;
        }

        if (vm.currentCluster.clusterName === undefined) return;
        accumuloSrv.getAvailableNodes(vm.currentCluster.clusterName).success(function (data) {
            vm.availableNodes = data;
        });
        ngDialog.open({
            template: 'plugins/accumulo/partials/addNodesForm.html',
            scope: $scope
        });
    }

    function addNode(chosenNode) {
        if (chosenNode === undefined) return;
        if (vm.currentCluster.clusterName === undefined) return;
        LOADING_SCREEN();
        SweetAlert.swal("Success!", "Node is being added.", "success");
        ngDialog.closeAll();
        accumuloSrv.addNode(vm.currentCluster.clusterName, chosenNode).success(function (data) {
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
        accumuloSrv.getHadoopClusters(selectedCluster).success(function (data) {
            vm.currentClusterNodes = data.slaves;
            vm.master = data.nameNode;
            LOADING_SCREEN('none');
        });
    }

    function createAccumulo() {
        if (vm.accumuloInstall.hadoopClusterName === undefined) {
            SweetAlert.swal("ERROR!", "Please select Hadoop cluster", "error");
        }
        else if (vm.accumuloInstall.master === undefined) {
            SweetAlert.swal("ERROR!", "Please set master node", "error");
        }
        else if (vm.accumuloInstall.nodes.length == 0) {
            SweetAlert.swal("ERROR!", "Please set slave nodes", "error");
        }
        else if (vm.accumuloInstall.password !== vm.confirmPassword) {
            SweetAlert.swal("ERROR!", "Passwords don\'t match", "error");
        }
        else {
            SweetAlert.swal("Success!", "Accumulo cluster is being created.", "success");
            LOADING_SCREEN();
            accumuloSrv.createAccumulo(vm.accumuloInstall).success(function (data) {
                SweetAlert.swal("Success!", "Your Accumulo cluster was created.", "success");
                LOADING_SCREEN("none");
                getClusters();
                vm.activeTab = 'manage';
            }).error(function (error) {
                SweetAlert.swal("ERROR!", 'Accumulo cluster creation error: ' + error.replace(/\\n/g, ' '), "error");
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
                    accumuloSrv.deleteCluster(vm.currentCluster.clusterName).success(function (data) {
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
                text: "This operation removes the Accumulo node from the cluster, and does not delete the container itself.",
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
                    accumuloSrv.deleteNode(vm.currentCluster.clusterName, nodeId).success(function (data) {
                        SweetAlert.swal("Deleted!", "Node has been removed.", "success");
                        getClustersInfo(vm.currentCluster.clusterName);
                    }).error(function (error) {
                        SweetAlert.swal("ERROR!", 'Failed to delete cluster node error: ' + error.replace(/\\n/g, ' '), "error");
                    });
                }
            });
    }

    function addContainer(containerId) {
        if (vm.accumuloInstall.nodes.indexOf(containerId) > -1) {
            vm.accumuloInstall.nodes.splice(vm.accumuloInstall.nodes.indexOf(containerId), 1);
        } else {
            vm.accumuloInstall.nodes.push(containerId);
        }
    }

    function setDefaultValues() {
        vm.accumuloInstall = {};
        vm.accumuloInstall.nodes = [];
        vm.accumuloInstall.server = {};
        vm.confirmPassword = "";
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
        vm.currentCluster.master.status = 'STARTING';
        accumuloSrv.startMasterNode(vm.currentCluster.clusterName, vm.currentCluster.master.id).success(function (data) {
            SweetAlert.swal("Success!", "Your master has been started.", "success");
            vm.currentCluster.master.status = 'RUNNING';
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Failed to start Accumulo master error: ' + error.replace(/\\n/g, ' '), "error");
            vm.currentCluster.master.status = 'ERROR';
        });
    }


    function stopMaster() {
        if (vm.currentCluster.clusterName === undefined) return;
        vm.currentCluster.master.status = 'STOPPING';
        accumuloSrv.stopMasterNode(vm.currentCluster.clusterName, vm.currentCluster.master.id).success(function (data) {
            SweetAlert.swal("Success!", "Your master has been stopped.", "success");
            vm.currentCluster.master.status = 'STOPPED';
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Failed to stop Accumulo master error: ' + error.replace(/\\n/g, ' '), "error");
            vm.currentCluster.master.status = 'ERROR';
        });
    }

    vm.info = {};
    accumuloSrv.getPluginInfo().success(function (data) {
        vm.info = data;
    });
}

function colSelectAccumuloNodes() {
    return {
        restrict: 'E',
        templateUrl: 'plugins/accumulo/directives/col-select/col-select-containers.html'
    }
};

