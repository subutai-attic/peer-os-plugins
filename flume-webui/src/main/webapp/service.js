'use strict';

angular.module('subutai.plugins.flume.service',[])
	.factory('flumeSrv', flumeSrv);

flumeSrv.$inject = ['$http', 'hadoopSrv'];

function flumeSrv($http, hadoopSrv) {
	var BASE_URL = SERVER_URL + 'rest/flume/';
	var CLUSTER_URL = BASE_URL + 'clusters/';

	var flumeSrv = {
		getHadoopClusters: getHadoopClusters,
		createFlume: createFlume,
		getClusters: getClusters,
		deleteNode: deleteNode,
		addNode: addNode,
		deleteCluster: deleteCluster,
		startNodes: startNodes,
		stopNodes: stopNodes,
		getAvailableNodes: getAvailableNodes,
	};

	return flumeSrv;

	function addNode(clusterName, lxcHostname) {
		return $http.post(CLUSTER_URL + clusterName + '/add/node/' + lxcHostname);
	}

	function getHadoopClusters(clusterName) {
		return hadoopSrv.getClusters(clusterName);
	}

	function getAvailableNodes(clusterName) {
		return $http.get(
			CLUSTER_URL + clusterName + '/available/nodes',
			{withCredentials: true, headers: {'Content-Type': 'application/json'}}
		);
	}

	function startNodes(clusterName, nodesArray) {
		var postData = 'clusterName=' + clusterName + '&lxcHosts=' + nodesArray;
		return $http.post(
			CLUSTER_URL + 'nodes/start',
			postData, 
			{withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
		);
	}

	function stopNodes(clusterName, nodesArray) {
		var postData = 'clusterName=' + clusterName + '&lxcHosts=' + nodesArray;
		return $http.post(
			CLUSTER_URL + 'nodes/stop',
			postData, 
			{withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
		);
	}

	function getClusters(clusterName) {
		if(clusterName === undefined || clusterName === null) clusterName = '';
		return $http.get(
			CLUSTER_URL + clusterName,
			{withCredentials: true, headers: {'Content-Type': 'application/json'}}
		);
	}

	function deleteCluster(clusterName) {
		return $http.delete(CLUSTER_URL + 'destroy/' + clusterName);
	}

	function deleteNode(clusterName, nodeId) {
		return $http.delete(CLUSTER_URL + clusterName + '/destroy/node/' + nodeId);
	}

	function createFlume(flumeObj) {
		console.log(flumeObj);
		var postData = 'clusterName=' + flumeObj.clusterName + '&hadoopClusterName=' + flumeObj.hadoopClusterName + '&nodes=' + JSON.stringify(flumeObj.nodes);
		return $http.post(
			BASE_URL,
			postData, 
			{withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
		);
	}
}
