package io.subutai.plugin.backup.rest;


import javax.ws.rs.core.Response;

import io.subutai.plugin.backup.api.Backup;


/**
 * Created by ermek on 2/24/16.
 */
public class RestServiceImpl implements RestService
{
    private Backup backup;


    public void setBackup( final Backup backup )
    {
        this.backup = backup;
    }


    @Override
    public Response executeBackup( String lxcHostName )
    {
        backup.executeBackup( lxcHostName );
        return null;
    }
}
