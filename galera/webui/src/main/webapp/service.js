'use strict';

angular.module('subutai.plugins.galera.service', [])
    .factory('galeraSrv', galeraSrv);

galeraSrv.$inject = ['$http', 'environmentService'];

function galeraSrv($http, environmentService) {

    var BASE_URL = SERVER_URL + 'rest/galera/';
    var CLUSTER_URL = BASE_URL + 'clusters/';

    var galeraSrv = {
        getClusters: getClusters,
        getContainers: getContainers,
        createGalera: createGalera,
        changeClusterScaling: changeClusterScaling,
        deleteCluster: deleteCluster,
        addNode: addNode,
        deleteNode: deleteNode,
        startNodes: startNodes,
        stopNodes: stopNodes,
        getEnvironments: getEnvironments
    };

    return galeraSrv;

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

    function deleteNode(clusterName, hostname) {
        return $http.delete(CLUSTER_URL + clusterName + '/node/' + hostname);
    }

    function getEnvironments() {
        return environmentService.getEnvironments();
    }

    function getClusters(clusterName) {
        if (clusterName === undefined || clusterName === null) clusterName = '';
        return $http.get(
            CLUSTER_URL + clusterName,
            {withCredentials: true, headers: {'Content-Type': 'application/json'}}
        );
    }

    function getContainers(envId) {
        return $http.get(
            BASE_URL + 'containers/' + envId,
            {withCredentials: true, headers: {'Content-Type': 'application/json'}}
        );
    }


    function createGalera(galeraJson) {

        var postData = 'clusterName=' + galeraJson.clusterName + '&environmentId=' + galeraJson.environmentId + '&nodes=' + galeraJson.containers.join();
        return $http.post(
            CLUSTER_URL + 'install',
            postData,
            {withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
        );
    }
}
