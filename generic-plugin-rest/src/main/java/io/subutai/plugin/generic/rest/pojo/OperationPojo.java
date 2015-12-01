package io.subutai.plugin.generic.rest.pojo;


public class OperationPojo
{
    private Long operationId;
    private String profileName;
    private String operationName;
    private String commandName;
    private String cwd;
    private String timeout;
    private Boolean daemon;
    private Boolean script;


    public String getOperationName()
    {
        return operationName;
    }


    public void setOperationName( final String operationName )
    {
        this.operationName = operationName;
    }


    public String getCommandName()
    {
        return commandName;
    }


    public void setCommandName( final String commandName )
    {
        this.commandName = commandName;
    }


    public String getCwd()
    {
        return cwd;
    }


    public void setCwd( final String cwd )
    {
        this.cwd = cwd;
    }


    public String getTimeout()
    {
        return timeout;
    }


    public void setTimeout( final String timeout )
    {
        this.timeout = timeout;
    }


    public Boolean getDaemon()
    {
        return daemon;
    }


    public void setDaemon( final Boolean daemon )
    {
        this.daemon = daemon;
    }


    public Long getOperationId()
    {
        return operationId;
    }


    public void setOperationId( final Long operationId )
    {
        this.operationId = operationId;
    }


    public Boolean getScript()
    {
        return script;
    }


    public void setScript( final Boolean script )
    {
        this.script = script;
    }


    public String getProfileName()
    {
        return profileName;
    }


    public void setProfileName( final String profileName )
    {
        this.profileName = profileName;
    }
}
