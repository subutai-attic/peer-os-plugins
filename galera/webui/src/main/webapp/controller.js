'use strict';

angular.module('subutai.plugins.galera.controller', [])
    .controller('GaleraCtrl', GaleraCtrl)
    .directive('colSelectContainers', colSelectContainers);

GaleraCtrl.$inject = ['galeraSrv', 'SweetAlert'];
function GaleraCtrl(galeraSrv, SweetAlert) {
    var vm = this;
    vm.activeTab = 'install';
    vm.selectedOption = {};
    vm.galeraInstall = {};
    vm.environments = [];
    vm.containers = [];

    vm.clusters = [];
    vm.currentCluster = {};
    vm.nodes2Action = [];

    //functions
    vm.showContainers = showContainers;
    vm.addContainer = addContainer;
    vm.createGalera = createGalera;

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
    galeraSrv.getEnvironments().success(function (data) {
        vm.environments = data;
    });
    function getClusters() {
        galeraSrv.getClusters().success(function (data) {
            vm.clusters = data;
            //getClustersInfo(data[0]);
        });
    }

    getClusters();

    function getClustersInfo(selectedCluster) {
        LOADING_SCREEN();
        vm.nodes2Action = [];
        galeraSrv.getClusters(selectedCluster).success(function (data) {
            vm.currentCluster = data;
            LOADING_SCREEN('none');
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Cluster info error: ' + error.replace(/\\n/g, ' '), "error");
            LOADING_SCREEN('none');
        });
    }

    function startNodes() {
        if (vm.nodes2Action.length == 0) return;
        if (vm.currentCluster.name === undefined) return;
        vm.globalChecker = false;
        SweetAlert.swal({
            title: 'Success!',
            text: 'Your request is in progress. You will be notified shortly.',
            timer: VARS_TOOLTIP_TIMEOUT,
            showConfirmButton: false
        });
        LOADING_SCREEN();
        galeraSrv.startNodes(vm.currentCluster.name, JSON.stringify(vm.nodes2Action)).success(function (data) {
            SweetAlert.swal("Success!", "Your cluster nodes started successfully.", "success");
            getClustersInfo(vm.currentCluster.name);
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Cluster start error: ' + error.replace(/\\n/g, ' '), "error");
            LOADING_SCREEN('none');
        });
    }

    function stopNodes() {
        if (vm.nodes2Action.length == 0) return;
        if (vm.currentCluster.name === undefined) return;
        vm.globalChecker = false;
        SweetAlert.swal({
            title: 'Success!',
            text: 'Your request is in progress. You will be notified shortly.',
            timer: VARS_TOOLTIP_TIMEOUT,
            showConfirmButton: false
        });
        LOADING_SCREEN();
        galeraSrv.stopNodes(vm.currentCluster.name, JSON.stringify(vm.nodes2Action)).success(function (data) {
            SweetAlert.swal("Success!", "Your cluster nodes stoped successfully.", "success");
            getClustersInfo(vm.currentCluster.name);
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Cluster stop error: ' + error.replace(/\\n/g, ' '), "error");
            LOADING_SCREEN('none');
        });
    }

    function pushNode(id) {
        if (vm.nodes2Action.indexOf(id) >= 0) {
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
        if (vm.currentCluster.name === undefined) return;
        LOADING_SCREEN();
        SweetAlert.swal("Success!", "Adding node action started.", "success");
        galeraSrv.addNode(vm.currentCluster.name).success(function (data) {
            SweetAlert.swal(
                "Success!",
                "Node has been added on cluster " + vm.currentCluster.name + ".",
                "success"
            );
            getClustersInfo(vm.currentCluster.name);
            LOADING_SCREEN('none');
        });
    }

    function deleteNode(hostname) {
        if (vm.currentCluster.name === undefined) return;
        SweetAlert.swal({
                title: "Are you sure?",
                text: "This operation removes the Galera node from the cluster, and does not delete the container itself.",
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
                    galeraSrv.deleteNode(vm.currentCluster.name, hostname).success(function (data) {
                        SweetAlert.swal("Deleted!", "Node has been removed.", "success");
                        getClustersInfo(vm.currentCluster.name);
                    });
                }
            });
    }

    function deleteCluster() {
        if (vm.currentCluster.name === undefined) return;
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
                    galeraSrv.deleteCluster(vm.currentCluster.name).success(function (data) {
                        SweetAlert.swal("Deleted!", "Cluster has been deleted.", "success");
                        vm.currentCluster = {};
                        getClusters();
                    });
                }
            });
    }

    function switchTab(tab) {

        if (tab == 'manager') {
            vm.activeTab = 'manage';
            getClusters();
        }
    }

    function createGalera() {
        SweetAlert.swal("Success!", "Galera cluster is being created.", "success");
        switchTab('manager');
        LOADING_SCREEN();
        galeraSrv.createGalera(vm.galeraInstall).success(function (data) {
            SweetAlert.swal("Success!", "Galera cluster created successfully", "success");
            LOADING_SCREEN("none");
            getClusters();
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Galera cluster create error: ' + error.replace(/\\n/g, ' '), "error");
            LOADING_SCREEN("none");
            getClusters();
        });
    }

    function changeClusterScaling(scale) {
        if (vm.currentCluster.name === undefined) return;
        try {
            galeraSrv.changeClusterScaling(vm.currentCluster.name, scale);
        } catch (e) {
        }
    }

    function showContainers(environmentId) {

        vm.containers = [];
        for (var i in vm.environments) {
            if (environmentId == vm.environments[i].id) {
                for (var j = 0; j < vm.environments[i].containers.length; j++) {
                    if (vm.environments[i].containers[j].templateName == 'galera') {
                        vm.containers.push(vm.environments[i].containers[j]);
                    }
                }
                break;
            }
        }
    }

    function addContainer(containerId) {
        if (vm.galeraInstall.containers.indexOf(containerId) > -1) {
            vm.galeraInstall.containers.splice(vm.galeraInstall.containers.indexOf(containerId), 1);
        } else {
            vm.galeraInstall.containers.push(containerId);
        }
    }

    function setDefaultValues() {
        vm.galeraInstall.containers = [];
    }
}

function colSelectContainers() {
    return {
        restrict: 'E',
        templateUrl: 'plugins/galera/directives/col-select/col-select-containers.html'
    }
};


