package io.subutai.plugin.usergrid.impl;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;


public class UsergridWebModule implements WebuiModule
{
	public static String NAME = "Usergrid";
	public static String IMG = "plugins/usergrid/usergrid.png";
	private static final Map<String, Integer> TEMPLATES_REQUIREMENT;

	static
	{
		TEMPLATES_REQUIREMENT = new HashMap<> ();
		TEMPLATES_REQUIREMENT.put ( "elasticsearch144", 1 );
		TEMPLATES_REQUIREMENT.put ( "tomcat7", 1 );
		TEMPLATES_REQUIREMENT.put ( "cassandra", 1 );
	}

	private WebuiModuleResourse appscaleResource;


	public void init ()
	{
		this.appscaleResource = new WebuiModuleResourse ( NAME.toLowerCase (), IMG );
		AngularjsDependency angularjsDependency = new AngularjsDependency (
				"subutai.plugins.usergrid",
				"plugins/usergrid/usergird.js",
				"plugins/usergrid/controller.js",
				"plugins/usergrid/service.js",
				"subutai-app/environment/service.js"
		);

		appscaleResource.addDependency ( angularjsDependency );
	}


	@Override
	public String getAngularState ()
	{
		return appscaleResource.getAngularjsList ();
	}


	@Override
	public String getName ()
	{
		return NAME;
	}


	@Override
	public String getModuleInfo ()
	{
		return String.format ( "{\"img\" : \"%s\", \"name\" : \"%s\", \"requirement\" : %s}", IMG, NAME,
				new Gson ().toJson ( TEMPLATES_REQUIREMENT ).toString () );
	}


	@Override
	public String getAngularDependecyList ()
	{
		return String.format ( ".state('%s', %s)", NAME.toLowerCase(), appscaleResource.getAngularjsList () );
	}
}
