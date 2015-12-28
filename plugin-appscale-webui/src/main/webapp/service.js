/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

'use strict';

angular.module('subutai.plugins.appscale.service', [])
    .factory('appscaleSrv', appscaleSrv);

appscaleSrv.$inject = ['$http', 'environmentService'];

function appscaleSrv ($http, $environmentService) {
    var BASE_URL = SERVER_URL + 'rest/appscale/';
    var CLUSTER_URL = BASE_URL + 'clusters/';
    var HADOOP_CREATE_URL = BASE_URL + 'configure_environment';
    
    var appscaleSrv = {
        getCluster : getCluster,
        configureCluster : configureCluster,
        uninstallCluster : uninstallCluster,
        getEnvironments: getEnvironments
    };
    return appscaleSrv;
    
    function getCluster (clusterName) {
        
    }
    
    function configureCluster () {
        
    }
    function uninstallCluster () {
        
    }
    
    function getEnvironments () {
        
    }
    
}