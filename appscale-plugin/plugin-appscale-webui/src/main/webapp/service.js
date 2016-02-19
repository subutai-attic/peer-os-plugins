'use strict';

angular.module('subutai.plugins.appscale.service', [])
	.factory('appscaleSrv', appscaleSrv);

appscaleSrv.$inject = ['$http', 'environmentService'];


function appscaleSrv ($http, environmentService) {
	var BASE_URL = SERVER_URL + 'rest/appscale/';
	var CLUSTERS_URL = BASE_URL + 'clusters/';
	var appscaleSrv = {
		listClusters: listClusters,
		build: build
	};
	return appscaleSrv;

	function build (config) {
		var postData = 'clusterName=' + config.master.hostname + '&zookeeperName=' + config.zookeeper.hostname + "&cassandraName=" + config.master.db.hostname;
		return $http.post(
			BASE_URL + 'configure_environment',
			postData,
			{withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
		);
	}

	function listClusters (clusterName) {
		return $http.get (CLUSTER_URL + clusterName, {withCredentials: true, headers: {'Content-Type': 'application/json'}});
	}
}