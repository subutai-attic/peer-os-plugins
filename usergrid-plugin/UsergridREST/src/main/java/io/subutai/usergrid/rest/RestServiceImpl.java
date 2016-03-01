/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.usergrid.rest;


import io.subutai.common.command.Response;
import io.subutai.common.environment.Environment;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class RestServiceImpl implements RestService
{
    @Override
    public Response configureCluster ( String clusterName )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public Response listClusterUI ( Environment environmentID )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public Response listClustersDB ()
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }

}

