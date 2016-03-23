package io.subutai.plugin.backup.rest;


import javax.ws.rs.core.Response;

import io.subutai.plugin.backup.api.Backup;
import io.subutai.plugin.backup.api.BackupException;


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
        try
        {
            backup.executeBackup( lxcHostName );
        }
        catch ( BackupException e )
        {
            e.printStackTrace();
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( e.getMessage() ).build();
        }
        return Response.ok().build();
    }
}
