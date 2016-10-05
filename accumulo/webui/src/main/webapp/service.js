'use strict';

angular.module('subutai.plugins.accumulo.service',[])
	.factory('accumuloSrv', accumuloSrv);

accumuloSrv.$inject = ['$http', 'hadoopSrv'];


function accumuloSrv($http, hadoopSrv) {
	var BASE_URL = SERVER_URL + 'rest/accumulo/';
	var CLUSTER_URL = BASE_URL + 'clusters/';

	var accumuloSrv = {
		getPluginInfo: getPluginInfo,
		getHadoopClusters: getHadoopClusters,
		createAccumulo: createAccumulo,
		getClusters: getClusters,
		deleteNode: deleteNode,
		getAvailableNodes: getAvailableNodes,
		addNode: addNode,
		deleteCluster: deleteCluster,
		startMasterNode: startMasterNode,
		stopMasterNode: stopMasterNode,
		startNodes: startNodes,
		stopNodes: stopNodes
	};

	return accumuloSrv;

	function addNode(clusterName, lxcHostId) {
		return $http.post(CLUSTER_URL + clusterName + '/add/node/' + lxcHostId);
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

	function deleteNode(clusterName, lxcHostId) {
		return $http.delete(CLUSTER_URL + clusterName + '/destroy/node/' + lxcHostId);
	}

	function createAccumulo(accumuloObj) {
		var postData = 'clusterName=' + accumuloObj.clusterName 
			+ '&hadoopClusterName=' + accumuloObj.hadoopClusterName 
			+ '&master=' + accumuloObj.master 
			+ "&slaves=" + JSON.stringify (accumuloObj.nodes)
			+ '&pwd=' + accumuloObj.password;
		return $http.post(
			BASE_URL,
			postData, 
			{withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
		);
	}

	function startMasterNode(clusterName, lxcHostId) {
		return $http.put (CLUSTER_URL + clusterName + "/start/node/" + lxcHostId );
	}

	function stopMasterNode(clusterName, lxcHostId) {
		return $http.put (CLUSTER_URL + clusterName + "/stop/node/" + lxcHostId );
	}

	function startNodes(clusterName, nodesArray) {
		var postData = 'clusterName=' + clusterName + '&lxcHostIds=' + nodesArray;
		return $http.post(
			CLUSTER_URL + 'nodes/start',
			postData, 
			{withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
		);
	}

	function stopNodes(clusterName, nodesArray) {
		var postData = 'clusterName=' + clusterName + '&lxcHostIds=' + nodesArray;
		return $http.post(
			CLUSTER_URL + 'nodes/stop',
			postData, 
			{withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
		);
	}

	function getPluginInfo() {
    	return $http.get (BASE_URL + "about", {withCredentials: true, headers: {'Content-Type': 'application/json'}});
    }
}
