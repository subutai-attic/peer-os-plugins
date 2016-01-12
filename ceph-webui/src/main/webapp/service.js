'use strict';

angular.module('subutai.plugins.ceph.service',[])
	.factory('cephSrv', cephSrv);

cephSrv.$inject = ['$http'];


function cephSrv($http, environmentService) {
	var BASE_URL = SERVER_URL + 'rest/ceph/';

	var cephSrv = {
		getEnvironments: getEnvironments,
		createEnvironment: createEnvironment
	};

	return cephSrv;

	function getEnvironments() {
		return $http.get (SERVER_URL + 'rest/ui/environments', {withCredentials: true, headers: {'Content-Type': 'application/json'}});
	}

	function createEnvironment (clusterName, environmentId, hostname) {
		var postData = "environmentId=" + environmentId + "&lxcHostName=" + hostname + "&clusterName=" + clusterName;
		console.log (postData);
		return $http.post (BASE_URL + "configure", postData, {withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}});
	}
}
