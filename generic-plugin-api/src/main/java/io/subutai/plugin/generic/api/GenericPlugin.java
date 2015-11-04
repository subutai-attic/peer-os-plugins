package io.subutai.plugin.generic.api;


import java.util.List;

import io.subutai.common.peer.ContainerHost;
import io.subutai.plugin.generic.api.model.Operation;


public interface GenericPlugin
{
    public void saveProfile( String profileName );

    public List<io.subutai.plugin.generic.api.model.Profile> getProfiles();

    public void saveOperation( Long profileId, String operationName, String commandName, String cwd, String timeout,
                               Boolean daemon );

    public List<Operation> getProfileOperations( Long profileId );

    public boolean IsOperationRegistered( String operationName );

    public void updateOperation( final Operation operation, final String commandValue, final String cwdValue,
                                 final String timeoutValue, final Boolean daemonValue );

    public void deleteOperation( Long operationId );

    public String executeCommandOnContainer( ContainerHost host, Operation operation );
}
