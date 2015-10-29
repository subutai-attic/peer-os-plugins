package io.subutai.plugin.generic.ui.wizard;

import com.vaadin.ui.Component;
import com.vaadin.ui.GridLayout;
import io.subutai.plugin.generic.api.Profile;
import io.subutai.plugin.generic.api.GenericPluginConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.registry.api.TemplateRegistry;

import java.util.ArrayList;


public class Wizard
{
	private GenericPluginConfiguration config = new GenericPluginConfiguration();
    private final GridLayout grid;
    private int step = 0; // 0 - welcome, 1 - create profile, 2 - configure operations, 3 - manage containers
	private TemplateRegistry registry;
	private EnvironmentManager manager;

    public Wizard (TemplateRegistry registry, EnvironmentManager manager)
    {
		this.registry = registry;
		this.manager = manager;
        grid = new GridLayout( 1, 20 );
        grid.setMargin( true );
        grid.setSizeFull();

        putForm();
    }


    public void putForm()
    {
        grid.removeComponent( 0, 1 );
        Component component = null;
        switch ( step )
        {
            case ( 0 ):
            {
                component = new WelcomeStep (this);
                break;
            }
            case ( 1 ):
            {
                component = new ProfileCreationStep (this);
                break;
            }
            case ( 2 ):
            {
                component = new ConfigureOperationStep (this);
                break;
            }
            case ( 3 ):
            {
                component = new ManageContainersStep (this);
                break;
            }
            default:
            {
                break;
            }
        }
        if ( component != null )
        {
            grid.addComponent( component, 0, 1, 0, 19 );
        }
    }

	protected void init()
	{
		step = 1;
		config = new GenericPluginConfiguration();
		putForm();
	}



	public void changeWindow( int newStep )
    {
        this.step = newStep;
    }


    public Component getContent()
    {
        return this.grid;
    }

    public GenericPluginConfiguration getConfig()
	{
		return this.config;
	}


	public TemplateRegistry getRegistry()
	{
		return this.registry;
	}


	public EnvironmentManager getManager()
	{
		return this.manager;
	}
}
