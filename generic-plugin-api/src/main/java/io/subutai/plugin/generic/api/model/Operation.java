package io.subutai.plugin.generic.api.model;


public interface Operation
{
    String getOperationName();


    void setOperationName( final String operationName );


    String getCommandName();


    void setCommandName( final String commandName );


    String getCwd();


    void setCwd( final String cwd );


    String getTimeout();


    void setTimeout( final String timeout );


    Boolean getDaemon();


    void setDaemon( final Boolean daemon );

    String getProfileName();


    void setProfileName( final String profileName );


    Long getOperationId();


    void setOperationId( final Long operationId );


    Boolean getScript();

    void setScript( final Boolean script );
}
