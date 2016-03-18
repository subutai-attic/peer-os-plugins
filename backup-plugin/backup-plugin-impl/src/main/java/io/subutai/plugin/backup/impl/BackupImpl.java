package io.subutai.plugin.backup.impl;


import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.peer.Host;
import io.subutai.common.peer.HostNotFoundException;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.plugin.backup.api.Backup;
import io.subutai.plugin.backup.api.BackupException;


/**
 * Created by ermek on 2/24/16.
 */
public class BackupImpl implements Backup
{
    private PeerManager peerManager;


    @Override
    public boolean executeBackup( final String lxcHostName ) throws BackupException
    {
        CommandResult commandResult = null;
        try
        {
            String command = String.format( "sudo subutai backup %s -full", lxcHostName );
            RequestBuilder requestBuilder = new RequestBuilder( command );
            Host host = peerManager.getLocalPeer().getManagementHost();
            commandResult = peerManager.getLocalPeer().execute( requestBuilder, host );
        }
        catch ( HostNotFoundException | CommandException e )
        {
            e.printStackTrace();
        }

        return commandResult != null && commandResult.hasSucceeded();
    }


    public void setPeerManager( final PeerManager peerManager )
    {
        this.peerManager = peerManager;
    }
}
