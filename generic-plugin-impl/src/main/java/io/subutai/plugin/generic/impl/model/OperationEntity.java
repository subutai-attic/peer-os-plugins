package io.subutai.plugin.generic.impl.model;


import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import io.subutai.plugin.generic.api.model.Operation;


@Entity
@Table (name = "plugin_operation")
@Access (AccessType.FIELD)
public class OperationEntity implements Operation
{
	@Id
	@Column (name = "operation_id")
	@GeneratedValue
	private Long operationId;

	@Column (name = "profile_id")
	private Long profileId;

	@Column (name = "operation_name")
	private String operationName;

	@Column (name = "command_name")
	private String commandName;

	@Column (name = "cwd")
	private String cwd;

	@Column (name = "timeout")
	private String timeout;

	@Column (name = "daemon")
	private Boolean daemon;

	@Column (name = "script")
	private Boolean script;


	public String getOperationName ()
	{
		return operationName;
	}


	public void setOperationName (final String operationName)
	{
		this.operationName = operationName;
	}


	public String getCommandName ()
	{
		return commandName;
	}


	public void setCommandName (final String commandName)
	{
		this.commandName = commandName;
	}


	public String getCwd ()
	{
		return cwd;
	}


	public void setCwd (final String cwd)
	{
		this.cwd = cwd;
	}


	public String getTimeout ()
	{
		return timeout;
	}


	public void setTimeout (final String timeout)
	{
		this.timeout = timeout;
	}


	public Boolean getDaemon ()
	{
		return daemon;
	}


	public void setDaemon (final Boolean daemon)
	{
		this.daemon = daemon;
	}


	public Long getProfileId ()
	{
		return profileId;
	}


	public void setProfileId (final Long profileId)
	{
		this.profileId = profileId;
	}


	public Long getOperationId ()
	{
		return operationId;
	}


	public void setOperationId (final Long operationId)
	{
		this.operationId = operationId;
	}


	public Boolean getScript ()
	{
		return script;
	}


	public void setScript (final Boolean script)
	{
		this.script = script;
	}
}
