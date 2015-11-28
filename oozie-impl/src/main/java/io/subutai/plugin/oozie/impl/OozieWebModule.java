package io.subutai.plugin.oozie.impl;


import io.subutai.webui.api.WebuiModule;

public class OozieWebModule implements WebuiModule
{
	public static String NAME = "Oozie";
	public static String IMG = "plugins/oozie/oozie.png";

	@Override
	public String getName()
	{
		return NAME;
	}


	@Override
	public String getModuleInfo()
	{
		return String.format( "{\"img\" : \"%s\", \"name\" : \"%s\"}", IMG, NAME );
	}


	@Override
	public String getAngularDependecyList()
	{
		return "{"
				+ "url: '/plugins/oozie',"
				+ "templateUrl: 'plugins/oozie/partials/view.html',"
				+ "resolve: {"
				+ "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {" + "return $ocLazyLoad.load(["
				+ "{"
				+ "name: 'subutai.plugins.oozie',"
				+ "files: ["
				+ "'plugins/oozie/oozie.js',"
				+ "'plugins/oozie/controller.js',"
				+ "'plugins/oozie/service.js'"
				+ "]"
				+ "}"
				+ "]);"
				+ "}]"
				+ "}";
	}
}
