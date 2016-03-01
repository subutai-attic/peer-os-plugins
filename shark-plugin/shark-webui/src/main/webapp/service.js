'use strict';

angular.module('subutai.plugins.shark.service',[])
	.factory('sharkSrv', sharkSrv);

sharkSrv.$inject = ['$http', 'sparkSrv'];


function sharkSrv($http, sparkSrv) {
	var BASE_URL = SERVER_URL + 'rest/shark/';
	var CLUSTER_URL = BASE_URL + 'clusters/';

	var sharkSrv = {
		getPluginInfo: getPluginInfo,
		getSparkClusters: getSparkClusters,
		createShark: createShark,
		getClusters: getClusters,
		deleteNode: deleteNode,
		getAvailableNodes: getAvailableNodes,
		addNode: addNode,
		deleteCluster: deleteCluster,
		startNode: startNode,
		stopNode: stopNode
	};

	return sharkSrv;

	function addNode(clusterName, lxcHostname) {
		return $http.post(CLUSTER_URL + clusterName + '/add/node/' + lxcHostname);
	}

	function getSparkClusters(clusterName) {
		return sparkSrv.getClusters(clusterName);
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

	function createShark(sharkObj) {
		var postData = 'clusterName=' + sharkObj.clusterName 
			+ '&sparkClusterName=' + sharkObj.sparkClusterName;
		return $http.post(
			CLUSTER_URL + 'install',
			postData, 
			{withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
		);
	}

	function startNode (clusterName, lxcHostName) {
		console.log (clusterName);
		console.log (lxcHostName);
		console.log (CLUSTER_URL + clusterName + "/start/node/" + lxcHostName);
		return $http.put (CLUSTER_URL + clusterName + "/start/node/" + lxcHostName);
	}

	function stopNode (clusterName, lxcHostName) {
		return $http.put (CLUSTER_URL + clusterName + "/stop/node/" + lxcHostName);
	}

	function getPluginInfo() {
    	return $http.get (BASE_URL + "about", {withCredentials: true, headers: {'Content-Type': 'application/json'}});
    }
}
