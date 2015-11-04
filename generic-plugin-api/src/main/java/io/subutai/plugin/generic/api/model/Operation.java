package io.subutai.plugin.generic.api.model;


public interface Operation
{
    public String getOperationName();


    public void setOperationName( final String operationName );


    public String getCommandName();


    public void setCommandName( final String commandName );


    public String getCwd();


    public void setCwd( final String cwd );


    public String getTimeout();


    public void setTimeout( final String timeout );


    public Boolean getDaemon();


    public void setDaemon( final Boolean daemon );

    public Long getProfileId();


    public void setProfileId( final Long profileId );


    public Long getOperationId();


    public void setOperationId( final Long operationId );
}
