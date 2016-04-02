package io.subutai.plugin.backup.impl;


import io.subutai.webui.api.WebuiModule;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class BackupWebModule implements WebuiModule
{

	public static String NAME = "Backup";
	public static String IMG = "plugins/backup/backup.png";


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
		return ".state('backup', {\n" + "            url: '/plugins/backup',\n"
				+ "            templateUrl: 'plugins/backup/partials/view.html',\n"
				+ "            data: {\n"
				+ "                bodyClass: '',\n"
				+ "                layout: 'default'\n"
				+ "            },\n"
				+ "            resolve: {\n"
				+ "                loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n"
				+ "                    return $ocLazyLoad.load([\n" + "                        {\n"
				+ "                            name: 'subutai.plugins.backup',\n"
				+ "                            files: [\n"
				+ "                                'plugins/backup/backup.js',\n"
				+ "                                'plugins/backup/controller.js',\n"
				+ "                                'plugins/backup/service.js',\n"
				+ "                                'subutai-app/environment/service.js'\n"
				+ "                            ]\n" + "                        }\n" + "                    ]);\n"
				+ "                }]\n" + "            }\n" + "        })";
	}
}

