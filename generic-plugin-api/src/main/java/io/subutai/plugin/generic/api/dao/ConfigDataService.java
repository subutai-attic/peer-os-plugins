package io.subutai.plugin.generic.api.dao;


import java.util.List;

import io.subutai.plugin.generic.api.model.Operation;
import io.subutai.plugin.generic.api.model.Profile;


public interface ConfigDataService
{
    void saveProfile( String profileName );

    List<Profile> getAllProfiles();

    void saveOperation( Long profileId, String operationName, String commandName, String cwd, String timeout,
                        Boolean daemon );

    List<Operation> getOperations( Long profileId );

    boolean isOperationRegistered( String operationName );

    Operation getOperationByName( String operationName );

    void updateOperation( Operation operation, String commandValue, String cwdValue, String timeoutValue,
                          Boolean daemonValue );

    void deleteOperation( Long operationId );

    Profile getProfileById( Long id );
}
