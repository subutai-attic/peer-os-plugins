angular.module('subutai.cassandra.service', [])
    .factory('cassandraService', environmentService);


cassandraService.$inject = ['$http'];

function cassandraService($http) {
    var BASE_URL = serverUrl + 'cassandra/';
    var clustersUrl = BASE_URL + '/clusters'

    var cassandraService = {
        getClusters : getClusters
    };

    return cassandraService;

    //// Implementation

    function getClusters() {
        return $http.get(clustersUrl, {withCredentials: true, headers: {'Content-Type': 'application/json'}});
    }
}
