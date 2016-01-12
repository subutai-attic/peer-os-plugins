package io.subutai.plugin.ceph.impl;

import io.subutai.webui.api.WebuiModule;

public class CephWebModule implements WebuiModule
{
	public static String NAME = "CEPH";
	public static String IMG = "plugins/ceph/ceph.png";

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
		return ".state('ceph', {\n" +
				"url: '/plugins/ceph',\n" +
				"templateUrl: 'plugins/ceph/partials/view.html',\n" +
				"resolve: {\n" +
				"loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n" +
				"return $ocLazyLoad.load([\n" +
				"{\n" +
				"name: 'subutai.plugins.ceph',\n" +
				"files: [\n" +
				"'plugins/ceph/ceph.js',\n" +
				"'plugins/ceph/controller.js',\n" +
				"'plugins/ceph/service.js',\n" +
				"]\n" +
				"}\n" +
				"]);\n" +
				"}]\n" +
				"}\n" +
				"})";
	}
}