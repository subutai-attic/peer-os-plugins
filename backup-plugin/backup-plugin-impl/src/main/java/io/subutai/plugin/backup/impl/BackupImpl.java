package io.subutai.plugin.backup.impl;


import io.subutai.common.command.CommandException;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.peer.Host;
import io.subutai.common.peer.HostNotFoundException;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.plugin.backup.api.Backup;


/**
 * Created by ermek on 2/24/16.
 */
public class BackupImpl implements Backup
{
    private PeerManager peerManager;


    @Override
    public void executeBackup( final String lxcHostName )
    {
        try
        {
            String command = String.format( "subutai backup %s [-full]", lxcHostName );
            RequestBuilder requestBuilder = new RequestBuilder( command );
            Host host = peerManager.getLocalPeer().getManagementHost();
            peerManager.getLocalPeer().execute( requestBuilder, host );
        }
        catch ( HostNotFoundException | CommandException e )
        {
            e.printStackTrace();
        }
    }


    public void setPeerManager( final PeerManager peerManager )
    {
        this.peerManager = peerManager;
    }
}
