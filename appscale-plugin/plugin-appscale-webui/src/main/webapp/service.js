'use strict';

angular.module('subutai.plugins.appscale.service', [])
	.factory('appscaleSrv', appscaleSrv);

appscaleSrv.$inject = ['$http', 'environmentService'];


function appscaleSrv ($http, environmentService) {
	var BASE_URL = SERVER_URL + 'rest/appscale/';
	var CLUSTERS_URL = BASE_URL + 'clusters/';
	var appscaleSrv = {
		getEnvironments: getEnvironments,
		build: build,
		listClusters: listClusters,
		getClusterInfo: getClusterInfo,
		uninstallCluster: uninstallCluster,
		quickInstall: quickInstall
	};
	return appscaleSrv;

	function getEnvironments() {
		return environmentService.getEnvironments();
	}



	function build (config) {
		var postData = 'clusterName=' + config.master.hostname + '&zookeeperName=' + config.zookeeper.join(",")
			+ "&cassandraName=" + config.db.join(",") + "&appengineName=" + config.appeng.join(",")
			+ "&envID=" + config.environment.id + "&userDomain=" + config.userDomain + '&scaleOption=' + config.scaleOption;

                return $http.post(
			BASE_URL + 'configure_environment',
			postData,
			{withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
		);
	}


	function listClusters() {
		return $http.get (BASE_URL + "clusterList", {withCredentials: true, headers: {'Content-Type': 'application/json'}});
	}

	function getClusterInfo (cluster) {
		return $http.get (BASE_URL + "clusters/" + cluster.clusterName, {withCredentials: true, headers: {'Content-Type': 'application/json'}});
	}

	function uninstallCluster (cluster) {
		return $http.delete (BASE_URL + "clusters/" + cluster.clusterName);
	}

	function quickInstall ( val ) {

		var postData = 'ename=' + val.name + '&udom=' + val.domain;

		return $http.post(
			BASE_URL + 'oneclick',
			postData,
			{withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
		);
	}
}