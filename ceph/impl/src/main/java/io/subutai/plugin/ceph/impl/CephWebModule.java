package io.subutai.plugin.ceph.impl;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;


public class CephWebModule implements WebuiModule
{
	private WebuiModuleResourse cephResource;
	public static String NAME = "Ceph";
	public static String IMG = "plugins/ceph/ceph.png";
	private static final Map<String, Integer> TEMPLATES_REQUIREMENT;
	static
	{
		TEMPLATES_REQUIREMENT = new HashMap<>();
		TEMPLATES_REQUIREMENT.put("ceph", 3);
	}


	public void init()
	{
		this.cephResource = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
		AngularjsDependency angularjsDependency = new AngularjsDependency(
				"subutai.plugins.ceph",
				"plugins/ceph/ceph.js",
				"plugins/ceph/controller.js",
				"plugins/ceph/service.js"
		);

		this.cephResource.addDependency(angularjsDependency);
	}


	@Override
	public String getModuleInfo()
	{
		return String.format( "{\"img\" : \"%s\", \"name\" : \"%s\", \"requirement\" : %s}", IMG, NAME, new Gson().toJson( TEMPLATES_REQUIREMENT ).toString() );
	}

	@Override
	public String getName()
	{
		return NAME;
	}

	@Override
	public String getAngularState()
	{
		return this.cephResource.getAngularjsList();
	}


	@Override
	public String getAngularDependecyList()
	{
		return String.format( ".state('%s', %s)", NAME.toLowerCase(), this.cephResource.getAngularjsList() );
	}
}