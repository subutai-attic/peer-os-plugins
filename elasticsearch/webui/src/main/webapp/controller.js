'use strict';

angular.module('subutai.plugins.elastic-search.controller', [])
    .controller('ElasticSearchCtrl', ElasticSearchCtrl)
    .directive('colSelectElasticSearchNodes', colSelectElasticSearchNodes);

ElasticSearchCtrl.$inject = ['$scope', 'elasticSearchSrv', 'SweetAlert', 'DTOptionsBuilder', 'DTColumnDefBuilder', 'ngDialog'];

function ElasticSearchCtrl($scope, elasticSearchSrv, SweetAlert, DTOptionsBuilder, DTColumnDefBuilder, ngDialog) {
    var vm = this;
    vm.activeTab = 'install';
    vm.elasticSearchInstall = {};
    vm.clusters = [];
    vm.environments = [];
    vm.containers = [];
    vm.currentCluster = [];
    vm.availableNodes = [];
    vm.selectedCluster = '';
    vm.nodes2Action = [];

    //functions
    vm.getClustersInfo = getClustersInfo;
    vm.addContainer = addContainer;
    vm.createElasticSearch = createElasticSearch;
    vm.deleteNode = deleteNode;
    vm.addNode = addNode;
    vm.pushNode = pushNode;
    vm.pushAll = pushAll;
    vm.deleteCluster = deleteCluster;
    vm.showContainers = showContainers;
    vm.changeClusterScaling = changeClusterScaling;
    vm.startNodes = startNodes;
    vm.stopNodes = stopNodes;

    function getEnvironments() {
        LOADING_SCREEN();
        elasticSearchSrv.getEnvironments().success(function (data) {
            vm.environments = data;
            LOADING_SCREEN("none");
        });
        setDefaultValues();
    }

    getEnvironments();

    function showContainers(environmentId) {
        vm.containers = [];
        vm.seeds = [];
        for (var i in vm.environments) {
            if (environmentId == vm.environments[i].id) {
                for (var j = 0; j < vm.environments[i].containers.length; j++) {
                    if (vm.environments[i].containers[j].templateName == 'elasticsearch235') {
                        vm.containers.push(vm.environments[i].containers[j]);
                    }
                }
                break;
            }
        }
    }

    function getClusters() {
        LOADING_SCREEN();
        elasticSearchSrv.getClusters().success(function (data) {
            vm.clusters = data;
            LOADING_SCREEN("none");
        });
    }

    getClusters();

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
        vm.nodes2Action = [];
        elasticSearchSrv.getClusters(selectedCluster).success(function (data) {
            vm.currentCluster = data;
            LOADING_SCREEN('none');
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Cluster getting info error: ' + error.replace(/\\n/g, ' '), "error");
            LOADING_SCREEN('none');
        });
    }

    function changeClusterScaling(scale) {
        if (vm.currentCluster.clusterName === undefined) return;
        try {
            elasticSearchSrv.changeClusterScaling(vm.currentCluster.clusterName, scale);
        } catch (e) {
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
        elasticSearchSrv.startNodes(vm.currentCluster.clusterName, JSON.stringify(vm.nodes2Action)).success(function (data) {
            SweetAlert.swal("Success!", "Your cluster nodes have been started successfully.", "success");
            getClustersInfo(vm.currentCluster.clusterName);
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Cluster starting error: ' + error.replace(/\\n/g, ' '), "error");
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
        elasticSearchSrv.stopNodes(vm.currentCluster.clusterName, JSON.stringify(vm.nodes2Action)).success(function (data) {
            SweetAlert.swal("Success!", "Your cluster nodes have been stopped successfully.", "success");
            getClustersInfo(vm.currentCluster.clusterName);
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Failed to stop cluster error: ' + error.replace(/\\n/g, ' '), "error");
        });
    }

    function pushNode(id) {
        if (vm.nodes2Action.indexOf(id) >= 0) {
            vm.nodes2Action.splice(vm.nodes2Action.indexOf(id), 1);
        } else {
            vm.nodes2Action.push(id);
        }
    }

    function pushAll() {
        vm.nodes2Action = [];
        if (vm.currentCluster.containers !== undefined) {
            if (vm.nodes2Action.length < vm.currentCluster.containers.length) {
                for (var i = 0; i < vm.currentCluster.containers.length; ++i) {
                    vm.nodes2Action.push(vm.currentCluster.containers[i].id);
                }
            }
        }
    }

    function addNode() {

        if (vm.currentCluster.environmentDataSource == "hub") {
            SweetAlert.swal("Feature coming soon...", "This environment created on Hub. Please use Hub to manage it.", "success");

            return;
        }

        if (vm.currentCluster.clusterName === undefined) return;
        LOADING_SCREEN();
        SweetAlert.swal("Success!", "Node is being added.", "success");
        ngDialog.closeAll();
        elasticSearchSrv.addNode(vm.currentCluster.clusterName).success(function (data) {
            SweetAlert.swal(
                "Success!",
                "Node has been added to cluster " + vm.currentCluster.clusterName + ".",
                "success"
            );
            getClustersInfo(vm.currentCluster.clusterName);
            LOADING_SCREEN('none');
        });
    }

    function createElasticSearch() {
        if (vm.elasticSearchInstall.environmentId === undefined) {
            SweetAlert.swal("ERROR!", 'Please select Elastic Search environment', "error");
        }
        else if (vm.elasticSearchInstall.nodes.length == 0) {
            SweetAlert.swal("ERROR!", "Please set nodes", "error");
        }
        else {
            SweetAlert.swal("Success!", "Elastic Search cluster is being created.", "success");
            LOADING_SCREEN();
            elasticSearchSrv.createElasticSearch(vm.elasticSearchInstall).success(function (data) {
                SweetAlert.swal("Success!", "Your Elastic Search cluster has been created.", "success");
                LOADING_SCREEN("none");
                getClusters();
                vm.activeTab = 'manage';
            }).error(function (error) {
                SweetAlert.swal("ERROR!", 'Elastic Search cluster creation error: ' + error.replace(/\\n/g, ' '), "error");
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
                    elasticSearchSrv.deleteCluster(vm.currentCluster.clusterName).success(function (data) {
                        SweetAlert.swal("Deleted!", "Cluster has been deleted.", "success");
                        vm.currentCluster = {};
                        getClusters();
                    }).error(function (error) {
                        SweetAlert.swal("ERROR!", 'Failed to delete cluster error: ' + error.replace(/\\n/g, ' '), "error");
                        vm.currentCluster = {};
                        getClusters();
                    });
                }
            });
    }

    function deleteNode(nodeId) {
        if (vm.currentCluster.clusterName === undefined) return;
        SweetAlert.swal({
                title: "Are you sure?",
                text: "This operation removes the ElasticSearch node from the cluster, and does not delete the container itself.",
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
                    elasticSearchSrv.deleteNode(vm.currentCluster.clusterName, nodeId).success(function (data) {
                        SweetAlert.swal("Deleted!", "Node has been removed.", "success");
                        getClustersInfo(vm.currentCluster.clusterName);
                    })
                    error(function (error) {
                        SweetAlert.swal("ERROR!", 'Delete cluster node error: ' + error.replace(/\\n/g, ' '), "error");
                    });
                }
            });
    }

    function addContainer(containerId) {
        if (vm.elasticSearchInstall.nodes.indexOf(containerId) > -1) {
            vm.elasticSearchInstall.nodes.splice(vm.elasticSearchInstall.nodes.indexOf(containerId), 1);
        } else {
            vm.elasticSearchInstall.nodes.push(containerId);
        }
    }

    function setDefaultValues() {
        vm.elasticSearchInstall = {};
        vm.elasticSearchInstall.nodes = [];
    }


    vm.info = {};
    elasticSearchSrv.getPluginInfo().success(function (data) {
        vm.info = data;
    });

}

function colSelectElasticSearchNodes() {
    return {
        restrict: 'E',
        templateUrl: 'plugins/elasticsearch/directives/col-select/col-select-containers.html'
    }
};

