/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

'use strict';

angular.module('subutai.plugins.appscale.service', [])
    .factory('appscaleSrv', appscaleSrv);

appscaleSrv.$inject = ['$http', 'environmentService'];

/*
 * SERVER_URL -> undefined?
 */
function appscaleSrv ($http, environmentService) {
    var BASE_URL = SERVER_URL + 'rest/appscale/';
    var CLUSTER_URL = BASE_URL + 'clusters/';
    var APPSCALE_CREATE_URL = BASE_URL + 'configure_environment';
    
    var appscaleSrv = {
        getCluster : getCluster,
        configureCluster : configureCluster,
        uninstallCluster : uninstallCluster,
        getEnvironments: getEnvironments
    };
    return appscaleSrv;
    
    function getCluster (clusterName) {
        if ( clusterName === undefined || clusterName === null ) clusterName = '';
        return $http.get (
                CLUSTER_URL + clusterName, {
                    withCredentials: true, headers: {'Content-Type:' : 'application/json'}}
                );
    }
    
    function configureCluster (appscaleJson) {
        var postData = 'config=' + appscaleJson;
        return $http.post(
            APPSCALE_CREATE_URL,
            postData,
            {withCredentials: true, headers: {'Content-Type': 'application/x-www-form-urlencoded'}}
        );
    }
    function uninstallCluster () {
        
    }
    
    function getEnvironments () {
        return environmentService.getEnvironments();
    }
    
}