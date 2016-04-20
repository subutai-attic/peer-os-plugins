package io.subutai.plugin.generic.impl;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;


public class GenericWebModule implements WebuiModule
{


	private WebuiModuleResourse genericResource;
	public static String NAME = "Generic";
	public static String IMG = "plugins/generic/generic.png";

	public void init()
	{
		this.genericResource = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
		AngularjsDependency angularjsDependency = new AngularjsDependency(
				"subutai.plugins.generic",
				"'plugins/generic/generic.js'",
				"'plugins/generic/controller.js'",
				"'plugins/generic/service.js'",
				"'subutai-app/environment/service.js'"
		);

		this.genericResource.addDependency(angularjsDependency);
	}


	@Override
	public String getModuleInfo()
	{
		return String.format( "{\"img\" : \"%s\", \"name\" : \"%s\"}", IMG, NAME );
	}

	@Override
	public String getName()
	{
		return NAME;
	}

	@Override
	public String getAngularState()
	{
		return this.genericResource.getAngularjsList();
	}


	@Override
	public String getAngularDependecyList()
	{
		return String.format( ".state('%s', %s)", NAME.toLowerCase(), this.genericResource.getAngularjsList() );
	}
}
