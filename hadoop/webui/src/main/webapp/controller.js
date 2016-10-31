'use strict';

angular.module('subutai.plugins.hadoop.controller', [])
    .controller('HadoopCtrl', HadoopCtrl)
    .directive('colSelectHadoopContainers', colSelectHadoopContainers)
    .directive('checkboxListDropdown', checkboxListDropdown);

HadoopCtrl.$inject = ['hadoopSrv', 'SweetAlert', 'DTOptionsBuilder', 'DTColumnDefBuilder'];

function HadoopCtrl(hadoopSrv, SweetAlert, DTOptionsBuilder, DTColumnDefBuilder) {
    var vm = this;
    vm.activeTab = 'install';
    vm.hadoopInstall = {};
    vm.environments = [];
    vm.containers = [];
    vm.clusters = [];
    vm.otherNodes = [];


    //functions
    vm.createHadoop = createHadoop;
    vm.showContainers = showContainers;
    vm.addContainer = addContainer;
    vm.getClustersInfo = getClustersInfo;
    vm.changeClusterScaling = changeClusterScaling;
    vm.deleteCluster = deleteCluster;
    vm.deleteNode = deleteNode;
    vm.addNode = addNode;
    vm.startNode = startNode;
    vm.stopNode = stopNode;
    vm.changeDirective = changeDirective;
    vm.addAllContainers = addAllContainers;
    vm.unselectAllContainers = unselectAllContainers;

    setDefaultValues();

    hadoopSrv.getEnvironments().success(function (data) {
        vm.environments = data;
    });

    function getClusters() {
        hadoopSrv.getClusters().success(function (data) {
            vm.clusters = data;
        });
    }

    getClusters();

    function getClustersInfo(selectedCluster) {
        LOADING_SCREEN();
        hadoopSrv.getClusters(selectedCluster).success(function (data) {
            vm.currentCluster = data;
            console.log(vm.currentCluster);
            LOADING_SCREEN('none');
        }).error(function (data) {
            console.log(data);
            LOADING_SCREEN('none');
        });
    }

    function changeClusterScaling(scale) {
        if (vm.currentCluster.clusterName === undefined) return;
        try {
            hadoopSrv.changeClusterScaling(vm.currentCluster.clusterName, scale);
        } catch (e) {
        }
    }

    function addNode() {

        if (vm.currentCluster.environmentDataSource == "hub") {
            SweetAlert.swal("Feature coming soon...", "This environment created on Hub. Please use Hub to manage it.", "success");

            return;
        }

        if (vm.currentCluster.clusterName === undefined) return;

        LOADING_SCREEN();

        SweetAlert.swal("Success!", "Node adding is in progress.", "success");

        hadoopSrv.addNode(vm.currentCluster.clusterName).success(function (data) {
            SweetAlert.swal(
                "Success!",
                "Node has been added to cluster " + vm.currentCluster.clusterName + ".",
                "success"
            );

            getClustersInfo(vm.currentCluster.clusterName);

            LOADING_SCREEN('none');
        });
    }

    function startNode(node, nodeType) {
        if (vm.currentCluster.clusterName === undefined) return;
        node.status = 'STARTING';
        hadoopSrv.startNode(vm.currentCluster.clusterName, node.hostname).success(function (data) {
            SweetAlert.swal("Success!", "Your cluster node have been started successfully.", "success");
            node.status = 'RUNNING';
            getClustersInfo(vm.currentCluster.clusterName);
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Failed to start cluster error: ' + error.replace(/\\n/g, ' '), "error");
            node.status = 'ERROR';
        });
    }

    function stopNode(node, nodeType) {
        if (vm.currentCluster.clusterName === undefined) return;
        node.status = 'STOPPING';
        hadoopSrv.stopNode(vm.currentCluster.clusterName, node.hostname).success(function (data) {
            SweetAlert.swal("Success!", "Your cluster node have stopped successfully.", "success");
            getClustersInfo(vm.currentCluster.clusterName);
            node.status = 'STOPPED';
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Failed to stop cluster error: ' + error.replace(/\\n/g, ' '), "error");
            node.status = 'ERROR';
        });
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
                    hadoopSrv.deleteCluster(vm.currentCluster.clusterName).success(function (data) {
                        SweetAlert.swal("Deleted!", "Cluster has been deleted.", "success");
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
                text: "This operation decommission the Hadoop node from the cluster, and does not delete the container itself.",
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
                    hadoopSrv.deleteNode(vm.currentCluster.clusterName, nodeId).success(function (data) {
                        SweetAlert.swal("Deleted!", "Node has been decommissioned.", "success");
                        getClustersInfo(vm.currentCluster.clusterName);
                    });
                }
            });
    }

    function createHadoop() {
        if (vm.hadoopInstall.environmentId === undefined || "") {
            SweetAlert.swal("ERROR!", 'Please select Hadoop environment', "error");
        }
        else if (vm.hadoopInstall.nameNode === undefined || "") {
            SweetAlert.swal("ERROR!", "Please set Namenode", "error");
        }
        else if (vm.hadoopInstall.slaves.length == 0) {
            SweetAlert.swal("ERROR!", "Please set Slave nodes", "error");
        }
        else {

            SweetAlert.swal("Success!", "Hadoop cluster is being created.", "success");
            LOADING_SCREEN();
            hadoopSrv.createHadoop(JSON.stringify(vm.hadoopInstall)).success(function (data) {
                SweetAlert.swal("Success!", "Hadoop cluster created successfully", "success");
                vm.activeTab = 'manage';
                getClusters();
                LOADING_SCREEN("none");
            }).error(function (error) {
                SweetAlert.swal("ERROR!", 'Hadoop cluster creation error: ' + error.replace(/\\n/g, ' '), "error");
                getClusters();
                LOADING_SCREEN("none");
            });
            setDefaultValues();
        }
    }

    function showContainers(environmentId) {
        vm.containers = [];
        vm.seeds = [];
        for (var i in vm.environments) {
            if (environmentId == vm.environments[i].id) {
                for (var j = 0; j < vm.environments[i].containers.length; j++) {
                    if (vm.environments[i].containers[j].templateName == 'hadoop') {
                        vm.containers.push(vm.environments[i].containers[j]);
                    }
                }
                break;
            }
        }
    }

    function addContainer(containerId) {
        if (vm.hadoopInstall.slaves.indexOf(containerId) > -1) {
            vm.hadoopInstall.slaves.splice(vm.hadoopInstall.slaves.indexOf(containerId), 1);
        } else {
            vm.hadoopInstall.slaves.push(containerId);
        }

        console.log(vm.hadoopInstall.slaves);
    }

    function addAllContainers() {
        vm.hadoopInstall.slaves = [];
        for (var i = 0; i < vm.containers.length; i++) {
            vm.hadoopInstall.slaves.push(vm.containers[i].id);
        }
    }

    function changeDirective(server) {
        vm.otherNodes = [];
        for (var i = 0; i < vm.containers.length; ++i) {
            if (vm.containers[i].id !== server) {
                vm.otherNodes.push(vm.containers[i]);
            }
        }
    }


    function unselectAllContainers() {
        vm.hadoopInstall.slaves = [];
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
        DTColumnDefBuilder.newColumnDef(3).notSortable()
    ];

    function setDefaultValues() {
        vm.hadoopInstall = {};
        vm.hadoopInstall.domainName = 'intra.lan';
        vm.hadoopInstall.replicationFactor = 1;
        vm.hadoopInstall.slaves = [];
    }


    vm.info = {};
    hadoopSrv.getPluginInfo().success(function (data) {
        vm.info = data;
    });
}

function colSelectHadoopContainers() {
    return {
        restrict: 'E',
        templateUrl: 'plugins/hadoop/directives/col-select/col-select-containers.html'
    }
};

function checkboxListDropdown() {
    return {
        restrict: 'A',
        link: function (scope, element, attr) {
            $(".b-form-input_dropdown").click(function () {
                $(this).toggleClass("is-active");
            });

            $(".b-form-input-dropdown-list").click(function (e) {
                e.stopPropagation();
            });
        }
    }
};

