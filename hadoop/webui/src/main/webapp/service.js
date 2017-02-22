'use strict';

angular.module('subutai.plugins.hadoop.service', [])
    .factory('hadoopSrv', hadoopSrv);

hadoopSrv.$inject = ['$http', 'environmentService'];
function hadoopSrv($http, environmentService) {

    var BASE_URL = SERVER_URL + 'rest/hadoop/';
    var CLUSTER_URL = BASE_URL + 'clusters/';
    var HADOOP_CREATE_URL = BASE_URL + 'configure_environment';

    var hadoopSrv = {
        getPluginInfo: getPluginInfo,
        createHadoop: createHadoop,
        getClusters: getClusters,
        getContainers: getContainers,
        changeClusterScaling: changeClusterScaling,
        deleteCluster: deleteCluster,
        deleteNode: deleteNode,
        addNode: addNode,
        startNode: startNode,
        stopNode: stopNode,
        getEnvironments: getEnvironments
    };

    return hadoopSrv;

    function getClusters(clusterName) {
        if (clusterName === undefined || clusterName === null) clusterName = '';
        return $http.get(
            CLUSTER_URL + clusterName,
            {withCredentials: true, headers: {'Content-Type': 'application/json'}}
        );
    }

    function addNode(clusterName) {
        return $http.post(CLUSTER_URL + clusterName + '/nodes');
    }

    function startNode(clusterName, hostname) {
        var postData = '';
        return $http.put(
            CLUSTER_URL + clusterName + '/start/' + hostname,
            postData,
            {withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
        );
    }

    function stopNode(clusterName, hostname) {
        var postData = '';
        return $http.put(
            CLUSTER_URL + clusterName + '/stop/' + hostname,
            postData,
            {withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
        );
    }

    function changeClusterScaling(clusterName, scale) {
        return $http.post(CLUSTER_URL + clusterName + '/auto_scale/' + scale);
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


    function deleteCluster(clusterName) {
        return $http.delete(CLUSTER_URL + clusterName);
    }

    function deleteNode(clusterName, nodeId) {
        return $http.delete(CLUSTER_URL + clusterName + '/remove/node/' + nodeId);
    }


    function createHadoop(hadoopJson) {
        var postData = 'config=' + hadoopJson;
        return $http.post(
            HADOOP_CREATE_URL,
            postData,
            {withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
        );
    }

    function getPluginInfo() {
        return $http.get(BASE_URL + "about", {withCredentials: true, headers: {'Content-Type': 'application/json'}});
    }
}
