'use strict';

angular.module('subutai.plugins.zookeeper.service',[])
	.factory('zookeeperSrv', zookeeperSrv);

zookeeperSrv.$inject = ['$http', 'environmentService', 'hadoopSrv'];

function zookeeperSrv($http, environmentService, hadoopSrv) {
	var BASE_URL = SERVER_URL + 'rest/zookeeper/';
	var CLUSTER_URL = BASE_URL + 'clusters/';

	var zookeeperSrv = {
		getPluginInfo: getPluginInfo,
		getEnvironments: getEnvironments,
		createZookeeper: createZookeeper,
		getClusters: getClusters,
		deleteNode: deleteNode,
		getAvailableNodes: getAvailableNodes,
		addNode: addNode,
		deleteCluster: deleteCluster,
		changeClusterScaling: changeClusterScaling,
		startNodes: startNodes,
		stopNodes: stopNodes,
		getHadoopClusters: getHadoopClusters
	};

	return zookeeperSrv;

	function addNode(clusterName, lxcHostname) {
		return $http.post(CLUSTER_URL + clusterName + '/add/node/' + lxcHostname);
	}

	function getEnvironments() {
		return environmentService.getEnvironments();
	}

	function getHadoopClusters(clusterName) {
		return hadoopSrv.getClusters(clusterName);
	}

	function getClusters(clusterName) {
		if(clusterName === undefined || clusterName === null) clusterName = '';
		return $http.get(
			CLUSTER_URL + clusterName,
			{withCredentials: true, headers: {'Content-Type': 'application/json'}}
		);
	}

	function getAvailableNodes(clusterName) {
		return $http.get(
			CLUSTER_URL + clusterName + '/available/nodes',
			{withCredentials: true, headers: {'Content-Type': 'application/json'}}
		);
	}

	function deleteCluster(clusterName) {
		return $http.delete(CLUSTER_URL + 'destroy/' + clusterName);
	}

	function deleteNode(clusterName, nodeId) {
		return $http.delete(CLUSTER_URL + clusterName + '/destroy/node/' + nodeId);
	}

	function createZookeeper(zookeeperObj, installType) {
		var postData = 'clusterName=' + zookeeperObj.clusterName 
			+ '&environmentId=' + zookeeperObj.environmentId
			+ "&nodes=" + JSON.stringify (zookeeperObj.nodes);
		var url = BASE_URL + 'configure_environment';
		if(installType == 'hadoop') {
			url = CLUSTER_URL + 'install';
			postData += '&hadoopClusterName=' + zookeeperObj.hadoopClusterName;
		}
		return $http.post(
			url,
			postData, 
			{withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
		);
	}

	function changeClusterScaling (clusterName, val) {
		return $http.post (CLUSTER_URL + clusterName + "/auto_scale/" + val);
	}

	function startNodes(clusterName, nodesArray) {
		var postData = 'clusterName=' + clusterName + '&lxcHostNames=' + nodesArray;
		return $http.post(
			CLUSTER_URL + 'nodes/start',
			postData,
			{withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
		);
	}

	function stopNodes(clusterName, nodesArray) {
		var postData = 'clusterName=' + clusterName + '&lxcHostNames=' + nodesArray;
		return $http.post(
			CLUSTER_URL + 'nodes/stop',
			postData,
			{withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
		);
	}

	function getAvailableNodes(clusterName) {
		return $http.get(
			CLUSTER_URL + clusterName + '/available/nodes',
			{withCredentials: true, headers: {'Content-Type': 'application/json'}}
		);
	}

	function getPluginInfo() {
    	return $http.get (BASE_URL + "about", {withCredentials: true, headers: {'Content-Type': 'application/json'}});
    }
}
