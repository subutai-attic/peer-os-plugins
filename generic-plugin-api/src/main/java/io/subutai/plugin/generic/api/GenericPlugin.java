package io.subutai.plugin.generic.api;


import java.util.List;

import io.subutai.common.peer.ContainerHost;
import io.subutai.plugin.generic.api.model.Operation;
import io.subutai.plugin.generic.api.model.Profile;


public interface GenericPlugin
{
    void saveProfile( String profileName );

    List<Profile> getProfiles();

    void saveOperation( Long profileId, String operationName, String commandName, String cwd, String timeout,
                        Boolean daemon );

    List<Operation> getProfileOperations( Long profileId );

    boolean IsOperationRegistered( String operationName );

    void updateOperation( final Operation operation, final String commandValue, final String cwdValue,
                          final String timeoutValue, final Boolean daemonValue );

    void deleteOperation( Long operationId );

    String executeCommandOnContainer( ContainerHost host, Operation operation );
}
