'use strict';

angular.module('subutai.plugins.hbase.controller', [])
    .controller('HbaseCtrl', HbaseCtrl)
    .directive('colSelectRegionHbaseNodes', colSelectRegionHbaseNodes);

HbaseCtrl.$inject = ['$scope', 'hbaseSrv', 'SweetAlert', 'DTOptionsBuilder', 'DTColumnDefBuilder', 'ngDialog'];

function HbaseCtrl($scope, hbaseSrv, SweetAlert, DTOptionsBuilder, DTColumnDefBuilder, ngDialog) {
    var vm = this;
    vm.activeTab = 'install';
    vm.hbaseInstall = {};
    vm.hbaseAll = false;
    vm.regionServers = [];
    vm.clusters = [];
    vm.hadoopClusters = [];
    vm.currentClusterNodes = [];
    vm.currentCluster = [];
    vm.availableNodes = [];
    vm.nodes2Action = [];
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
    vm.startMaster = startMaster;
    vm.stopMaster = stopMaster;
    vm.pushNode = pushNode;
    vm.pushAll = pushAll;


    hbaseSrv.getHadoopClusters().success(function (data) {
        vm.hadoopClusters = data;
        if (vm.hadoopClusters.length == 0) {
            SweetAlert.swal("ERROR!", 'No Hadoop clusters was found! Create Hadoop cluster first.', "error");
        }
    }).error(function (error) {
        SweetAlert.swal("ERROR!", 'No Hadoop clusters was found! ERROR: ' + error.replace(/\\n/g, ' '), "error");
    });
    setDefaultValues();

    function getClusters() {
        LOADING_SCREEN();
        hbaseSrv.getClusters().success(function (data) {
            vm.clusters = data;
            LOADING_SCREEN("none");
        });
    }

    getClusters();

    vm.dtOptions = DTOptionsBuilder
        .newOptions()
        .withOption('order', [[3, "asc"]])
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
            for (var i = 0; i < vm.currentCluster.regionServers.length; ++i) {
                vm.currentCluster.regionServers[i].checkbox = false;
            }
            LOADING_SCREEN('none');
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Get cluster info error: ' + error.replace(/\\n/g, ' '), "error");
            LOADING_SCREEN('none');
        });
    }

    function changeClusterScaling(scale) {
        if (vm.currentCluster.clusterName === undefined) return;
        try {
            hbaseSrv.changeClusterScaling(vm.currentCluster.clusterName, scale);
        } catch (e) {
        }
    }

    function pushNode(id) {
        if (vm.nodes2Action.indexOf(id) >= 0) {
            vm.nodes2Action.splice(vm.nodes2Action.indexOf(id), 1);
            vm.hbaseAll = false;
        } else {
            vm.nodes2Action.push(id);
            if (vm.nodes2Action.length === vm.currentCluster.regionServers.length) {
                vm.hbaseAll = true;
            }
        }
    }


    function pushAll() {
        if (vm.currentCluster.regionServers !== undefined) {
            if (vm.nodes2Action.length === vm.currentCluster.regionServers.length) {
                vm.nodes2Action = [];
                vm.hbaseAll = false;
                for (var i = 0; i < vm.currentCluster.regionServers.length; ++i) {
                    vm.currentCluster.regionServers[i].checkbox = false;
                }
            }
            else {
                for (var i = 0; i < vm.currentCluster.regionServers.length; ++i) {
                    vm.nodes2Action.push(vm.currentCluster.regionServers[i].hostname);
                    vm.currentCluster.regionServers[i].checkbox = true;
                }
                vm.hbaseAll = true;
            }
        }
    }


    function startNodes() {
        if (vm.nodes2Action.length == 0) return;
        if (vm.currentCluster.clusterName === undefined) return;
        LOADING_SCREEN();
        SweetAlert.swal({
            title: 'Success!',
            text: 'Your request is in progress. You will be notified shortly.',
            timer: VARS_TOOLTIP_TIMEOUT,
            showConfirmButton: false
        });
        hbaseSrv.startNodes(vm.currentCluster.clusterName, JSON.stringify(vm.nodes2Action)).success(function (data) {
            SweetAlert.swal("Success!", "Your cluster slaves started successfully.", "success");
            vm.nodes2Action = [];
            vm.hbaseAll = false;
            getClustersInfo(vm.currentCluster.clusterName);
            LOADING_SCREEN('none');
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Cluster slaves start error: ' + error.replace(/\\n/g, ' '), "error");
        });
    }

    function stopNodes() {
        if (vm.nodes2Action.length == 0) return;
        if (vm.currentCluster.clusterName === undefined) return;
        LOADING_SCREEN();
        SweetAlert.swal({
            title: 'Success!',
            text: 'Your request is in progress. You will be notified shortly.',
            timer: VARS_TOOLTIP_TIMEOUT,
            showConfirmButton: false
        });
        hbaseSrv.stopNodes(vm.currentCluster.clusterName, JSON.stringify(vm.nodes2Action)).success(function (data) {
            SweetAlert.swal("Success!", "Your cluster slaves have stopped successfully.", "success");
            vm.nodes2Action = [];
            vm.hbaseAll = false;
            getClustersInfo(vm.currentCluster.clusterName);
            LOADING_SCREEN('none');
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Failed to stop cluster slaves error: ' + error.replace(/\\n/g, ' '), "error");
        });
    }

    function addNodeForm() {
        if (vm.currentCluster.clusterName === undefined) return;
        hbaseSrv.getAvailableNodes(vm.currentCluster.clusterName).success(function (data) {
            vm.availableNodes = data;
        });
        ngDialog.open({
            template: 'plugins/hbase/partials/addNodesForm.html',
            scope: $scope
        });
    }

    function addNode(chosenNode) {
        if (chosenNode === undefined) return;
        if (vm.currentCluster.clusterName === undefined) return;
        LOADING_SCREEN();
        SweetAlert.swal("Success!", "Adding node action started.", "success");
        ngDialog.closeAll();
        hbaseSrv.addNode(vm.currentCluster.clusterName, chosenNode).success(function (data) {
            SweetAlert.swal(
                "Success!",
                "Node has been added on cluster " + vm.currentCluster.clusterName + ".",
                "success"
            );
            getClustersInfo(vm.currentCluster.clusterName);
            LOADING_SCREEN('none');
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Adding node error: ' + error.replace(/\\n/g, ' '), "error");
        });
    }

    function changeDirective(hmaster) {
        vm.otherNodes = [];
        for (var i = 0; i < vm.currentClusterNodes.length; ++i) {
            if (vm.currentClusterNodes[i].uuid !== hmaster) {
                vm.otherNodes.push(vm.currentClusterNodes[i]);
            }
        }
    }

    function getHadoopClusterNodes(selectedCluster) {
        LOADING_SCREEN();
        hbaseSrv.getHadoopClusters(selectedCluster).success(function (data) {
            vm.hadoopFullInfo = data;
            vm.currentClusterNodes = data.slaves;
            vm.currentClusterNodes.push(data.nameNode);
            vm.hbaseInstall.namenode = data.nameNode;
            LOADING_SCREEN('none');
        });
    }

    function createHbase() {
        if (vm.hbaseInstall.clusterName === undefined || vm.hbaseInstall.clusterName.length == 0) return;
        if (vm.hbaseInstall.hadoopClusterName === undefined || vm.hbaseInstall.hadoopClusterName.length == 0) return;

        vm.hbaseInstall.environmentId = vm.hadoopFullInfo.environmentId;
        vm.hbaseInstall.domainName = 'intra.lan';

        SweetAlert.swal("Success!", "Hbase cluster start creating.", "success");
        LOADING_SCREEN();
        hbaseSrv.createHbase(JSON.stringify(vm.hbaseInstall)).success(function (data) {
            SweetAlert.swal("Success!", "Your Hbase cluster created successfully.", "success");
            LOADING_SCREEN("none");
            getClusters();
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Hbase cluster create error: ' + error.replace(/\\n/g, ' '), "error");
            LOADING_SCREEN();
            getClusters();
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
                    hbaseSrv.deleteCluster(vm.currentCluster.clusterName).success(function (data) {
                        SweetAlert.swal("Deleted!", "Cluster has been deleted.", "success");
                        vm.currentCluster = {};
                        getClusters();
                    }).error(function (error) {
                        SweetAlert.swal("ERROR!", 'Delete cluster error: ' + error.replace(/\\n/g, ' '), "error");
                    });
                }
            });
    }

    function deleteNode(nodeId) {
        if (vm.currentCluster.clusterName === undefined) return;
        SweetAlert.swal({
                title: "Are you sure?",
                text: "This operation removes the Hbase node from the cluster, and does not delete the container itself.",
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
                        SweetAlert.swal("Deleted!", "Node has been removed.", "success");
                        getClustersInfo(vm.currentCluster.clusterName);
                    }).error(function (error) {
                        SweetAlert.swal("ERROR!", 'Delete node error: ' + data.replace(/\\n/g, ' '), "error");
                    });
                }
            });
    }

    function addContainer(containerId, field) {
        if (vm.hbaseInstall[field].indexOf(containerId) > -1) {
            vm.hbaseInstall[field].splice(vm.hbaseInstall[field].indexOf(containerId), 1);
        } else {
            vm.hbaseInstall[field].push(containerId);
        }
    }


    function startMaster() {
        if (vm.currentCluster.clusterName === undefined) return;
        vm.currentCluster.hbaseMaster.status = 'STARTING';
        hbaseSrv.startMasterNode(vm.currentCluster.clusterName, vm.currentCluster.hbaseMaster.hostname).success(function (data) {
            SweetAlert.swal("Success!", "Your HbaseMaster has been started.", "success");
            vm.currentCluster.hbaseMaster.status = 'RUNNING';
            getClustersInfo(vm.currentCluster.clusterName);
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Failed to start HbaseMaster error: ' + error.replace(/\\n/g, ' '), "error");
            vm.currentCluster.hbaseMaster.status = 'ERROR';
        });
    }


    function stopMaster() {
        if (vm.currentCluster.clusterName === undefined) return;
        vm.currentCluster.hbaseMaster.status = 'STOPPING';
        hbaseSrv.stopMasterNode(vm.currentCluster.clusterName, vm.currentCluster.hbaseMaster.hostname).success(function (data) {
            SweetAlert.swal("Success!", "Your HbaseMaster has been stopped.", "success");
            vm.currentCluster.hbaseMaster.status = 'STOPPED';
            getClustersInfo(vm.currentCluster.clusterName);
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Failed to stop HbaseMaster error: ' + error.replace(/\\n/g, ' '), "error");
            vm.currentCluster.hbaseMaster.status = 'ERROR';
        });
    }


    function setDefaultValues() {
        vm.hbaseInstall = {};
        vm.hbaseInstall.regionServers = [];
        vm.hbaseInstall.quorumPeers = [];
        vm.hbaseInstall.backupMasters = [];
        vm.hadoopFullInfo = {};
    }

    vm.info = {};
    hbaseSrv.getPluginInfo().success(function (data) {
        vm.info = data;
    });

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

