'use strict';

angular.module('subutai.plugins.cassandra.controller', [])
    .controller('CassandraCtrl', CassandraCtrl)
    .directive('colSelectContainers', colSelectContainers)
    .directive('colSelectSeeds', colSelectSeeds);

CassandraCtrl.$inject = ['cassandraSrv', 'SweetAlert', 'DTOptionsBuilder', 'DTColumnDefBuilder', '$timeout'];
function CassandraCtrl(cassandraSrv, SweetAlert, DTOptionsBuilder, DTColumnDefBuilder, $timeout) {
    var vm = this;
    vm.activeTab = 'install';
    vm.cassandraInstall = {};
    vm.environments = [];
    vm.containers = [];
    vm.seeds = [];

    vm.clusters = [];
    vm.currentCluster = {};
    vm.nodes2Action = [];

    //functions
    vm.showContainers = showContainers;
    vm.addContainer = addContainer;
    vm.addAllContainers = addAllContainers;
    vm.unselectAllContainers = unselectAllContainers;
    vm.addSeed = addSeed;
    vm.addAllSeeds = addAllSeeds;
    vm.unselectAllSeeds = unselectAllSeeds;
    vm.createCassandra = createCassandra;

    vm.getClustersInfo = getClustersInfo;
    vm.changeClusterScaling = changeClusterScaling;
    vm.deleteCluster = deleteCluster;
    vm.addNode = addNode;
    vm.deleteNode = deleteNode;
    vm.pushNode = pushNode;
    vm.startNodes = startNodes;
    vm.stopNodes = stopNodes;
    vm.pushAll = pushAll;

    setDefaultValues();
    cassandraSrv.getEnvironments().success(function (data) {
        vm.environments = data;
    });

    function getClusters() {
        vm.clusters = [];
        LOADING_SCREEN();
        cassandraSrv.getClusters().success(function (data) {
            vm.clusters = data;
            LOADING_SCREEN("none");
        }).error(function (error) {
            console.log(error);
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
        DTColumnDefBuilder.newColumnDef(4).notSortable(),
        DTColumnDefBuilder.newColumnDef(5).notSortable()
    ];

    /*function reloadTableData() {
     vm.refreshTable = $timeout(function myFunction() {
     if(typeof(vm.dtInstance.reloadData) == 'function') {
     vm.dtInstance.reloadData(null, false);
     }
     vm.refreshTable = $timeout(reloadTableData, 3000);
     }, 3000);
     };
     reloadTableData();*/

    function getClustersInfo(selectedCluster) {
        LOADING_SCREEN();
        vm.currentCluster = {};
        vm.nodes2Action = [];
        cassandraSrv.getClusters(selectedCluster).success(function (data) {
            console.log(data);
            vm.currentCluster = data;
            LOADING_SCREEN('none');
        });
    }

    function startNodes() {
        if (vm.nodes2Action.length == 0) return;
        if (vm.currentCluster.name === undefined) return;
        SweetAlert.swal({
            title: 'Success!',
            text: 'Your request is in progress. You will be notified shortly.',
            timer: VARS_TOOLTIP_TIMEOUT,
            showConfirmButton: false
        });
        LOADING_SCREEN();
        cassandraSrv.startNodes(vm.currentCluster.name, JSON.stringify(vm.nodes2Action)).success(function (data) {
            SweetAlert.swal("Success!", "Your cluster nodes started successfully.", "success");
            console.log("getting cluster info");
            getClustersInfo(vm.currentCluster.name);
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Cluster start error: ' + error.replace(/\\n/g, ' '), "error");
            LOADING_SCREEN("none");
        });
    }

    function stopNodes() {
        if (vm.nodes2Action.length == 0) return;
        if (vm.currentCluster.name === undefined) return;
        SweetAlert.swal({
            title: 'Success!',
            text: 'Your request is in progress. You will be notified shortly.',
            timer: VARS_TOOLTIP_TIMEOUT,
            showConfirmButton: false
        });
        LOADING_SCREEN();
        cassandraSrv.stopNodes(vm.currentCluster.name, JSON.stringify(vm.nodes2Action)).success(function (data) {
            SweetAlert.swal("Success!", "Your cluster nodes stoped successfully.", "success");
            console.log("getting cluster info");
            getClustersInfo(vm.currentCluster.name);
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Cluster stop error: ' + error.replace(/\\n/g, ' '), "error");
            LOADING_SCREEN("none");
        });
    }

    function pushNode(id) {
        if (vm.nodes2Action.indexOf(id) >= 0) {
            vm.nodes2Action.splice(vm.nodes2Action.indexOf(id), 1);
        } else {
            vm.nodes2Action.push(id);
        }
    }

    function addNode() {

        if (vm.currentCluster.environmentDataSource == "hub") {
            SweetAlert.swal("Feature coming soon...", "This environment created on Hub. Please use Hub to manage it.", "success");

            return;
        }

        if (vm.currentCluster.name === undefined) return;
        LOADING_SCREEN();
        SweetAlert.swal("Success!", "Adding node action started.", "success");
        cassandraSrv.addNode(vm.currentCluster.name).success(function (data) {
            SweetAlert.swal(
                "Success!",
                "Node has been added on cluster " + vm.currentCluster.name + ".",
                "success"
            );
            getClusters();
            vm.activeTab = 'manage';
            setDefaultValues();
            getClustersInfo(vm.currentCluster.name);
            LOADING_SCREEN("none");
        });
    }

    function deleteNode(nodeId) {
        if (vm.currentCluster.name === undefined) return;
        SweetAlert.swal({
                title: "Are you sure?",
                text: "This operation removes the Cassandra node from the cluster, and does not delete the container itself.",
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
                    cassandraSrv.deleteNode(vm.currentCluster.name, nodeId).success(function (data) {
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
                    cassandraSrv.deleteCluster(vm.currentCluster.name).success(function (data) {
                        SweetAlert.swal("Deleted!", "Cluster has been deleted.", "success");
                        vm.currentCluster = {};
                        getClusters();
                    });
                }
            });
    }

    function createCassandra() {
        if (vm.cassandraInstall.environmentId === undefined) {
            SweetAlert.swal("ERROR!", 'Please select Cassandra environment', "error");
        }
        else if (vm.cassandraInstall.containers.length == 0) {
            SweetAlert.swal("ERROR!", "Please set nodes for configuration", "error");
        }
        // else if (vm.cassandraInstall.seeds.length == 0) {
        //     SweetAlert.swal("ERROR!", "Please set Seed nodes", "error");
        // }
        else {
            SweetAlert.swal("Success!", "Your Cassandra cluster started creating.", "success");
            LOADING_SCREEN();
            cassandraSrv.createCassandra(JSON.stringify(vm.cassandraInstall)).success(function (data) {
                vm.activeTab = "manage";
                LOADING_SCREEN('none');
                SweetAlert.swal("Success!", "Your Cassandra cluster successfully created.", "success");
                getClusters();
            }).error(function (error) {
                SweetAlert.swal("ERROR!", 'Cassandra cluster create error: ' + error.replace(/\\n/g, ' '), "error");
                LOADING_SCREEN("none");
            });
        }

    }

    function changeClusterScaling(scale) {
        if (vm.currentCluster.name === undefined) return;
        try {
            cassandraSrv.changeClusterScaling(vm.currentCluster.name, scale);
        } catch (e) {
        }
    }

    function showContainers(environmentId) {
        vm.containers = [];
        vm.seeds = [];

        cassandraSrv.getContainers(environmentId).success(function (data) {
            vm.containers = data;
        });
    }

    function addContainer(containerId) {
        console.log(vm.containers);
        if (vm.cassandraInstall.containers.indexOf(containerId) > -1) {
            vm.cassandraInstall.containers.splice(vm.cassandraInstall.containers.indexOf(containerId), 1);
        } else {
            vm.cassandraInstall.containers.push(containerId);
        }
        vm.seeds = angular.copy(vm.cassandraInstall.containers);
    }

    function addAllContainers() {
        vm.cassandraInstall.containers = [];
        for (var i = 0; i < vm.containers.length; i++) {
            vm.cassandraInstall.containers.push(vm.containers[i].hostname);
        }
        console.log(vm.cassandraInstall.containers);
        vm.seeds = angular.copy(vm.cassandraInstall.containers);
    }

    function unselectAllContainers() {
        vm.cassandraInstall.containers = [];
        vm.cassandraInstall.seeds = [];
        vm.seeds = [];
    }

    function addSeed(seedId) {
        if (vm.cassandraInstall.seeds.indexOf(seedId) > -1) {
            vm.cassandraInstall.seeds.splice(vm.cassandraInstall.seeds.indexOf(seedId), 1);
        } else {
            vm.cassandraInstall.seeds.push(seedId);
        }
    }
    
    function addAllSeeds() {
        vm.cassandraInstall.seeds = [];
        for (var i = 0; i < vm.seeds.length; i++) {
            vm.cassandraInstall.seeds.push(vm.seeds[i]);
        }
    }
    
    function unselectAllSeeds() {
        vm.cassandraInstall.seeds = [];
    }

    function setDefaultValues() {
        vm.cassandraInstall.domainName = 'intra.lan';
        vm.cassandraInstall.dataDir = '/var/lib/cassandra/data';
        vm.cassandraInstall.commitDir = '/var/lib/cassandra/commitlog';
        vm.cassandraInstall.cacheDir = '/var/lib/cassandra/saved_caches';
        vm.cassandraInstall.containers = [];
        vm.cassandraInstall.seeds = [];
    }


    function pushAll() {
        if (vm.currentCluster.name !== undefined) {
            if (vm.nodes2Action.length === vm.currentCluster.containers.length) {
                vm.nodes2Action = [];
            }
            else {
                for (var i = 0; i < vm.currentCluster.containers.length; ++i) {
                    vm.nodes2Action.push(vm.currentCluster.containers[i]);
                }
            }
            console.log(vm.nodes2Action);
        }
    }


    vm.info = {};
    cassandraSrv.getPluginInfo().success(function (data) {
        vm.info = data;
    });
}

function colSelectContainers() {
    return {
        restrict: 'E',
        templateUrl: 'plugins/cassandra/directives/col-select/col-select-containers.html'
    }
};

function colSelectSeeds() {
    return {
        restrict: 'E',
        templateUrl: 'plugins/cassandra/directives/col-select/col-select-seeds.html'
    }
};

