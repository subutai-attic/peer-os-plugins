package io.subutai.plugin.generic.api;


import java.util.List;

import io.subutai.common.peer.ContainerHost;
import io.subutai.plugin.generic.api.dao.ConfigDataService;
import io.subutai.plugin.generic.api.model.Operation;
import io.subutai.plugin.generic.api.model.Profile;


public interface GenericPlugin
{
    void saveProfile( String profileName );

    List<Profile> getProfiles();

    void saveOperation( String profileName, String operationName, String commandName, String cwd, String timeout,
                        Boolean daemon, Boolean fromFile );

    List<Operation> getProfileOperations( String profileName );

    void updateOperation( final Long operationId, final String commandValue, final String cwdValue,
                          final String timeoutValue, final Boolean daemonValue, final Boolean fromFile, final String operationName );

    void deleteOperation( Long operationId );

    String executeCommandOnContainer( ContainerHost host, Operation operation );

    ConfigDataService getConfigDataService();

    void deleteProfile( String profileName );

    void deleteOperations( Long profileId );
}
