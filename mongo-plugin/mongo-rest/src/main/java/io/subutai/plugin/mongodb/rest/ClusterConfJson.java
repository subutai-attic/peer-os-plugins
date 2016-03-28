package io.subutai.plugin.mongodb.rest;


import java.util.Map;
import java.util.Set;

public class ClusterConfJson
{
	private String name;
	private String domainName;
	private String repl;
	private String configPort;
	private String routePort;
	private String dataPort;
	private Set<String> configNodes;
	private Set<String> routeNodes;
	private Set<String> dataNodes;
	private String environmentId;
	private boolean scaling;
	private Map<String, ContainerInfoJson> containersStatuses;


	public boolean isScaling ()
	{
		return scaling;
	}


	public void setScaling (final boolean scaling)
	{
		this.scaling = scaling;
	}


	public String getName ()
	{
		return name;
	}


	public void setName (final String name)
	{
		this.name = name;
	}


	public String getDomainName ()
	{
		return domainName;
	}


	public void setDomainName (final String domainName)
	{
		this.domainName = domainName;
	}

	public String getRepl ()
	{
		return repl;
	}

	public void setRepl (String repl)
	{
		this.repl = repl;
	}

	public String getDataPort ()
	{
		return dataPort;
	}

	public void setDataPort (String dataPort)
	{
		this.dataPort = dataPort;
	}

	public Set<String> getConfigNodes ()
	{
		return configNodes;
	}

	public void setConfigNodes (Set<String> configNodes)
	{
		this.configNodes = configNodes;
	}

	public String getConfigPort ()
	{
		return configPort;
	}

	public void setConfigPort (String configPort)
	{
		this.configPort = configPort;
	}

	public Set<String> getRouteNodes ()
	{
		return routeNodes;
	}

	public void setRouteNodes (Set<String> routeNodes)
	{
		this.routeNodes = routeNodes;
	}

	public Set<String> getDataNodes ()
	{
		return dataNodes;
	}

	public void setDataNodes (Set<String> dataNodes)
	{
		this.dataNodes = dataNodes;
	}

	public String getEnvironmentId ()
	{
		return environmentId;
	}

	public void setEnvironmentId (String environmentId)
	{
		this.environmentId = environmentId;
	}

	public Map<String, ContainerInfoJson> getContainersStatuses ()
	{
		return containersStatuses;
	}

	public void setContainersStatuses (Map<String, ContainerInfoJson> containersStatuses)
	{
		this.containersStatuses = containersStatuses;
	}

	public String getRoutePort ()
	{
		return routePort;
	}

	public void setRoutePort (String routePort)
	{
		this.routePort = routePort;
	}
}
