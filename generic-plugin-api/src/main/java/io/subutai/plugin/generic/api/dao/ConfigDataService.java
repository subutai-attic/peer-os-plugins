package io.subutai.plugin.generic.api.dao;


import java.util.List;

import io.subutai.plugin.generic.api.model.Operation;
import io.subutai.plugin.generic.api.model.Profile;


public interface ConfigDataService
{
    public void saveProfile( String profileName );

    public List<Profile> getAllProfiles();

    public void saveOperation( Long profileId, String operationName, String commandName, String cwd, String timeout,
                               Boolean daemon );

    public List<Operation> getOperations( Long profileId );

    public boolean isOperationRegistered( String operationName );

    public Operation getOperationByName( String operationName );

    public void updateOperation( Operation operation, String commandValue, String cwdValue, String timeoutValue,
                          Boolean daemonValue );

    public void deleteOperation( Long operationId );
}
