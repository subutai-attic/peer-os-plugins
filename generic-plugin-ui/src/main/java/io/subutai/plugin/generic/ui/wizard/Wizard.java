package io.subutai.plugin.generic.ui.wizard;


import com.vaadin.ui.Component;
import com.vaadin.ui.GridLayout;

import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.registry.api.TemplateRegistry;
import io.subutai.plugin.generic.api.GenericPlugin;


public class Wizard
{
    private final GridLayout grid;
    private int step = 0; // 0 - welcome, 1 - create profile, 2 - configure operations, 3 - manage containers
    private TemplateRegistry registry;
    private EnvironmentManager manager;
    private GenericPlugin genericPlugin;


    public Wizard( TemplateRegistry registry, EnvironmentManager manager, GenericPlugin genericPlugin )
    {
        this.registry = registry;
        this.manager = manager;
        this.genericPlugin = genericPlugin;
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
                component = new WelcomeStep( this, genericPlugin );
                break;
            }
            case ( 1 ):
            {
                component = new ProfileCreationStep( this, genericPlugin );
                break;
            }
            case ( 2 ):
            {
                component = new ConfigureOperationStep( this, genericPlugin );
                break;
            }
            case ( 3 ):
            {
                component = new ManageContainersStep( this, genericPlugin );
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


    public void changeWindow( int newStep )
    {
        this.step = newStep;
    }


    public Component getContent()
    {
        return this.grid;
    }



	/* public TemplateRegistry getRegistry()
    {
		return this.registry;
	}*/


    public EnvironmentManager getManager()
    {
        return this.manager;
    }


    public GenericPlugin getGenericPlugin()
    {
        return this.genericPlugin;
    }
}
