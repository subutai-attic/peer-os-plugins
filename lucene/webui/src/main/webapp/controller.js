'use strict';

angular.module('subutai.plugins.lucene.controller', [])
    .controller('LuceneCtrl', LuceneCtrl)
    .directive('colSelectLuceneNodes', colSelectLuceneNodes);

LuceneCtrl.$inject = ['$scope', 'luceneSrv', 'SweetAlert', 'DTOptionsBuilder', 'DTColumnDefBuilder', 'ngDialog'];

function LuceneCtrl($scope, luceneSrv, SweetAlert, DTOptionsBuilder, DTColumnDefBuilder, ngDialog) {
    var vm = this;
    vm.activeTab = 'install';
    vm.luceneInstall = {};
    vm.clusters = [];
    vm.hadoopClusters = [];
    vm.currentClusterNodes = [];
    vm.currentCluster = [];
    vm.availableNodes = [];

    //functions
    vm.getClustersInfo = getClustersInfo;
    vm.getHadoopClusterNodes = getHadoopClusterNodes;
    vm.addContainer = addContainer;
    vm.createLucene = createLucene;
    vm.deleteNode = deleteNode;
    vm.addNodeForm = addNodeForm;
    vm.addNode = addNode;
    vm.deleteCluster = deleteCluster;

    luceneSrv.getHadoopClusters().success(function (data) {
        vm.hadoopClusters = data;
        console.log(vm.hadoopClusters);
        if (vm.hadoopClusters.length == 0) {
            SweetAlert.swal("ERROR!", 'No Hadoop clusters was found! Create Hadoop cluster first.', "error");
        }
    }).error(function (error) {
        SweetAlert.swal("ERROR!", 'No Hadoop clusters was found! ERROR: ' + error.replace(/\\n/g, ' '), "error");
    });
    setDefaultValues();

    function getClusters() {
        LOADING_SCREEN();
        luceneSrv.getClusters().success(function (data) {
            vm.clusters = data;
            LOADING_SCREEN("none");
        });
    }

    getClusters();

    vm.dtOptions = DTOptionsBuilder
        .newOptions()
        .withOption('order', [[0, "asc"]])
        .withOption('stateSave', true)
        .withPaginationType('full_numbers');

    vm.dtColumnDefs = [
        DTColumnDefBuilder.newColumnDef(0),
        DTColumnDefBuilder.newColumnDef(1),
        DTColumnDefBuilder.newColumnDef(2).notSortable()
    ];

    function getClustersInfo(selectedCluster) {
        LOADING_SCREEN();
        luceneSrv.getClusters(selectedCluster).success(function (data) {
            vm.currentCluster = data;
            LOADING_SCREEN('none');
        });
    }

    function addNodeForm() {
        if (vm.currentCluster.clusterName === undefined) return;
        LOADING_SCREEN();
        luceneSrv.getAvailableNodes(vm.currentCluster.clusterName).success(function (data) {
            vm.availableNodes = data;
            LOADING_SCREEN('none');
        });
        ngDialog.open({
            template: 'plugins/lucene/partials/addNodesForm.html',
            scope: $scope
        });
    }

    function addNode(chosenNode) {
        if (chosenNode === undefined) return;
        if (vm.currentCluster.clusterName === undefined) return;
        LOADING_SCREEN();
        SweetAlert.swal("Success!", "Adding node action started.", "success");
        ngDialog.closeAll();
        luceneSrv.addNode(vm.currentCluster.clusterName, chosenNode).success(function (data) {
            SweetAlert.swal(
                "Success!",
                "Node has been added on cluster " + vm.currentCluster.clusterName + ".",
                "success"
            );
            getClustersInfo(vm.currentCluster.clusterName);
            LOADING_SCREEN('none');
        });
    }

    function getHadoopClusterNodes(selectedCluster) {
        LOADING_SCREEN();
        luceneSrv.getHadoopClusters(selectedCluster).success(function (data) {
            vm.currentClusterNodes = data.slaves;
            vm.currentClusterNodes.push(data.nameNode);
            LOADING_SCREEN('none');
            console.log(vm.currentClusterNodes);
        });
    }

    function createLucene() {
        if (vm.luceneInstall.clusterName === undefined || vm.luceneInstall.clusterName.length == 0) return;
        if (vm.luceneInstall.hadoopClusterName === undefined || vm.luceneInstall.hadoopClusterName.length == 0) return;

        SweetAlert.swal(
            {
                title: 'In progress',
                text: 'Your Lucene cluster creation is started!',
                timer: VARS_TOOLTIP_TIMEOUT,
                showConfirmButton: false
            }
        );
        LOADING_SCREEN();
        luceneSrv.createLucene(vm.luceneInstall).success(function (data) {
            SweetAlert.swal("Success!", "Your Lucene cluster has been created.", "success");
            LOADING_SCREEN("none");
            getClusters();
        }).error(function (error) {
            SweetAlert.swal("ERROR!", 'Lucene cluster create error: ' + error.replace(/\\n/g, ' '), "error");
            LOADING_SCREEN("none");
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
                    luceneSrv.deleteCluster(vm.currentCluster.clusterName).success(function (data) {
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
                    luceneSrv.deleteNode(vm.currentCluster.clusterName, nodeId).success(function (data) {
                        SweetAlert.swal("Deleted!", "Node has been deleted.", "success");
                        getClustersInfo(vm.currentCluster.clusterName);
                    });
                }
            });
    }

    function addContainer(containerId) {
        if (vm.luceneInstall.nodes.indexOf(containerId) > -1) {
            vm.luceneInstall.nodes.splice(vm.luceneInstall.nodes.indexOf(containerId), 1);
        } else {
            vm.luceneInstall.nodes.push(containerId);
        }
    }

    function setDefaultValues() {
        vm.luceneInstall = {};
        vm.luceneInstall.nodes = [];
    }

    vm.info = {};
    luceneSrv.getPluginInfo().success(function (data) {
        vm.info = data;
    });
}

function colSelectLuceneNodes() {
    return {
        restrict: 'E',
        templateUrl: 'plugins/lucene/directives/col-select/col-select-containers.html'
    }
};

