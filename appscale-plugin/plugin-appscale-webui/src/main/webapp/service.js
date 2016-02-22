'use strict';

angular.module('subutai.plugins.appscale.service', [])
	.factory('appscaleSrv', appscaleSrv);

appscaleSrv.$inject = ['$http', 'environmentService'];


function appscaleSrv ($http, environmentService) {
	var BASE_URL = SERVER_URL + 'rest/appscale/';
	var CLUSTERS_URL = BASE_URL + 'clusters/';
	var appscaleSrv = {
		getEnvironments: getEnvironments,
		build: build
	};
	return appscaleSrv;

        function getEnvironments() {
            return environmentService.getEnvironments();
        }
	function build (config) {
		var postData = 'clusterName=' + config.master.hostname + '&zookeeperName=' + config.zookeeper.hostname + "&cassandraName=" + config.db.hostname + "&envID=" + config.environment.id;
                console.log (postData);
                return $http.post(
			BASE_URL + 'configure_environment',
			postData,
			{withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
		);
	}
}