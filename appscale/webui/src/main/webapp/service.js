'use strict';

angular.module('subutai.plugins.appscale.service', [])
    .factory('appscaleSrv', appscaleSrv);

appscaleSrv.$inject = ['$http', 'environmentService'];


function appscaleSrv($http, environmentService) {
    var BASE_URL = SERVER_URL + 'rest/appscale/';
    var CLUSTERS_URL = BASE_URL + 'clusters/';
    var appscaleSrv = {
        getPluginInfo: getPluginInfo,
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


    function build(config) {
        var domain = config.userDomain;
        if (config.domainOption == 1) {
            domain += '.subut.ai';
        }
        var zkp = [];
        for (var i = 0; i < config.zookeeper.length; ++i) {
            zkp.push(config.zookeeper[i].ip);
        }
        var app = [];
        for (var i = 0; i < config.appeng.length; ++i) {
            app.push(config.appeng[i].ip);
        }
        var cass = [];
        for (var i = 0; i < config.db.length; ++i) {
            cass.push(config.db[i].ip);
        }
        var postData = 'clusterName=' + config.master.hostname + '&zookeeperName=' + zkp.join(",")
            + "&cassandraName=" + cass.join(",") + "&appengineName=" + app.join(",")
            + "&envID=" + config.environment.id + "&userDomain=" + domain + '&scaleOption=' + config.scaleOption + "&login=" + config.login + "&password=" + config.password;

        return $http.post(
            BASE_URL + 'configure_environment',
            postData,
            {withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
        );
    }


    function listClusters() {
        return $http.get(BASE_URL + "clusterList", {
            withCredentials: true,
            headers: {'Content-Type': 'application/json'}
        });
    }

    function getClusterInfo(cluster) {
        return $http.get(BASE_URL + "clusters/" + cluster.clusterName, {
            withCredentials: true,
            headers: {'Content-Type': 'application/json'}
        });
    }

    function uninstallCluster(cluster) {
        return $http.delete(BASE_URL + "clusters/" + cluster.clusterName);
    }

    function quickInstall(val) {

        var postData = 'ename=' + val.name + '&udom=' + val.domain;

        return $http.post(
            BASE_URL + 'oneclick',
            postData,
            {withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
        );
    }

    function getPluginInfo() {
        return $http.get(BASE_URL + "about", {withCredentials: true, headers: {'Content-Type': 'application/json'}});
    }

}