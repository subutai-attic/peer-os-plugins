'use strict';

angular.module('subutai.plugins.solr.service', [])
    .factory('solrSrv', solrSrv);

solrSrv.$inject = ['$http', 'environmentService'];

function solrSrv($http, environmentService) {

    var BASE_URL = SERVER_URL + 'rest/solr/';
    var CLUSTER_URL = BASE_URL + 'clusters/';

    var solrSrv = {
        getClusters: getClusters,
        getContainers: getContainers,
        createSolr: createSolr,
        changeClusterScaling: changeClusterScaling,
        deleteCluster: deleteCluster,
        addNode: addNode,
        deleteNode: deleteNode,
        startNodes: startNodes,
        stopNodes: stopNodes,
        getEnvironments: getEnvironments
    };

    return solrSrv;

    function addNode(clusterName) {
        return $http.post(CLUSTER_URL + clusterName + '/add');
    }

    function startNodes(clusterName, nodesArray) {
        var postData = 'clusterName=' + clusterName + '&lxcHosts=' + nodesArray;
        var url = CLUSTER_URL + 'nodes/start';

        return $http.post(
            url,
            postData,
            {withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
        );
        /*
         return $http.get(
         url,
         {withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
         );
         */
    }

    function stopNodes(clusterName, nodesArray) {
        var postData = 'clusterName=' + clusterName + '&lxcHosts=' + nodesArray;
        return $http.post(
            CLUSTER_URL + 'nodes/stop',
            postData,
            {withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
        );
    }

    function changeClusterScaling(clusterName, scale) {
        return $http.post(CLUSTER_URL + clusterName + '/auto_scale/' + scale);
    }

    function deleteCluster(clusterName) {
        return $http.delete(CLUSTER_URL + "destroy/" + clusterName);
    }

    function deleteNode(clusterName, nodeId) {
        return $http.delete(CLUSTER_URL + clusterName + '/node/' + nodeId);
    }

    function getEnvironments() {
        return environmentService.getEnvironments();
    }

    function getContainers(envId) {
        return $http.get(
            BASE_URL + 'containers/' + envId,
            {withCredentials: true, headers: {'Content-Type': 'application/json'}}
        );
    }


    function getClusters(clusterName) {
        if (clusterName === undefined || clusterName === null) clusterName = '';
        return $http.get(
            CLUSTER_URL + clusterName,
            {withCredentials: true, headers: {'Content-Type': 'application/json'}}
        );
    }

    function createSolr(solrJson) {

        var postData = 'clusterName=' + solrJson.clusterName + '&environmentId=' + solrJson.environmentId + '&nodes=' + solrJson.containers.join();
        return $http.post(
            CLUSTER_URL + 'install',
            postData,
            {withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
        );
    }
}
