package org.safehaus.subutai.plugin.etl.ui;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.sqoop.api.SqoopConfig;
import org.safehaus.subutai.plugin.sqoop.api.setting.CommonSetting;

import com.vaadin.ui.AbstractTextField;
import com.vaadin.ui.CheckBox;
import com.vaadin.ui.Component;
import com.vaadin.ui.ComponentContainer;
import com.vaadin.ui.Field;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.TextArea;
import com.vaadin.ui.VerticalLayout;


public abstract class ImportExportBase extends VerticalLayout
{

    private final Tracker tracker;
    protected String clusterName;
    protected ContainerHost host;
    protected List<Field> fields = new ArrayList<>();
    AbstractTextField connStringField;
    AbstractTextField tableField;
    AbstractTextField usernameField;
    AbstractTextField passwordField;
    AbstractTextField optionalParams;
    TextArea std_logs;
    TextArea std_err_logs;
    private String hostNameTitle = "";


    public String getHostNameTitle()
    {
        return hostNameTitle;
    }


    public void setHostNameTitle( final String hostNameTitle )
    {
        this.hostNameTitle = hostNameTitle;
    }


    protected ImportExportBase( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public String getClusterName()
    {
        return clusterName;
    }


    public void setClusterName( String clusterName )
    {
        this.clusterName = clusterName;
    }


    public ContainerHost getHost()
    {
        return host;
    }


    public void setHost( ContainerHost host )
    {
        this.host = host;
        reset();
    }


    void reset()
    {
        for ( Field f : this.fields )
        {
            if ( f instanceof AbstractTextField )
            {
                f.setValue( "" );
            }
            else if ( f instanceof CheckBox )
            {
                f.setValue( false );
            }
        }
    }


    abstract CommonSetting makeSettings();


    void init()
    {
        connStringField = UIUtil.getTextField( "Connection string:" );
        connStringField.setDescription( "Connection string to be used while connecting to " +
                                        "relational database.\n\n" +
                                        "e.g. jdbc:mysql://localhost:3306");

        tableField = UIUtil.getTextField( "Table name:" );
        usernameField = UIUtil.getTextField( "Username:" );
        passwordField = UIUtil.getTextField( "Password:", true );

        optionalParams = UIUtil.getTextField( "Optional parameters:" );
        optionalParams.setDescription( "Check out following page for all sqoop parameters: " +
                "http://sqoop.apache.org/docs/1.4.5/SqoopUserGuide.html. Do not forget to use \"--\" " +
                "(double dashes) in optional parameters." );

        fields.add( connStringField );
        fields.add( tableField );
        fields.add( usernameField );
        fields.add( passwordField );
        fields.add( optionalParams );
    }


    public GridLayout addComponentsVertical( List<Component> components )
    {
        GridLayout rootLayout = new GridLayout( 2, 1 );
        rootLayout.setSizeFull();
        rootLayout.setSpacing( true );

        VerticalLayout left = new VerticalLayout();
        VerticalLayout right = new VerticalLayout();
        left.setSizeFull();
        right.setSizeFull();
        left.setSpacing( true );
        right.setSpacing( true );

        for ( int i = 0; i < components.size(); i++ )
        {
            left.addComponent( components.get( i ) );
        }

        int rowSize = 30;
        std_logs = new TextArea();
        std_logs.setSizeFull();
        std_logs.setRows( rowSize );

        std_err_logs = new TextArea();
        std_err_logs.setSizeFull();
        std_err_logs.setRows( rowSize );

        final TabSheet tabsheet = new TabSheet();
        tabsheet.setCaption( "Query Results" );
        tabsheet.setSizeFull();

        final VerticalLayout tab1 = new VerticalLayout();
        tab1.setSizeFull();
        tab1.setSpacing( true );
        tab1.setCaption( "Output" );
        tabsheet.addTab(tab1);
        tab1.addComponent( std_logs );

        final VerticalLayout tab2 = new VerticalLayout();
        tab2.setSizeFull();
        tab2.setSpacing( true );
        tab2.setCaption( "Errors");
        tabsheet.addTab(tab2);
        tab2.addComponent( std_err_logs );

        right.addComponent( tabsheet );


        rootLayout.addComponent( left, 0, 0 );
        rootLayout.addComponent( right, 1, 0 );
        addComponent( rootLayout );
        return rootLayout;
    }


    boolean checkFields()
    {
        if ( !hasValue( connStringField, "Connection string not specified" ) )
        {
            return false;
        }
        // table check is done in subclasses
        if ( !hasValue( usernameField, "Username not specified" ) )
        {
            return false;
        }
        if ( !hasValue( passwordField, "Password not specified" ) )
        {
            return false;
        }
        // fields have value
        return true;
    }


    boolean hasValue( Field f, String errMessage )
    {
        if ( f.getValue() == null || f.getValue().toString().isEmpty() )
        {
            appendLogMessage( errMessage );
            return false;
        }
        return true;
    }


    void appendLogMessage( String m )
    {
        if ( m != null && m.length() > 0 )
        {
            std_logs.setValue( std_logs.getValue() + "\n" + m );
            std_logs.setCursorPosition( std_logs.getValue().length() );
        }
    }


    void setFieldsEnabled( boolean enabled )
    {
        for ( Field f : this.fields )
        {
            f.setEnabled( enabled );
        }
    }


    void clearLogMessages()
    {
        std_logs.setValue( "" );
        std_err_logs.setValue( "" );
    }


    void detachFromParent()
    {
        ComponentContainer parent = ( ComponentContainer ) getParent();
        parent.removeComponent( this );
    }


    protected interface OperationCallback
    {
        void onComplete();
    }


    protected class OperationWatcher implements Runnable
    {

        private final UUID trackId;
        private OperationCallback callback;


        public OperationWatcher( UUID trackId )
        {
            this.trackId = trackId;
        }


        public void setCallback( OperationCallback callback )
        {
            this.callback = callback;
        }


        @Override
        public void run()
        {
            String m = "";
            while ( true )
            {
                TrackerOperationView po = tracker.getTrackerOperation( SqoopConfig.PRODUCT_KEY, trackId );
                if ( po == null )
                {
                    break;
                }

                if ( po.getLog() != null )
                {
                    String logText = po.getLog().replace( m, "" );
                    m = po.getLog();
                    if ( !logText.isEmpty() )
                    {
                        appendLogMessage( logText );
                    }
                    if ( po.getState() != OperationState.RUNNING )
                    {
                        break;
                    }
                }
                try
                {
                    Thread.sleep( 300 );
                }
                catch ( InterruptedException ex )
                {
                    break;
                }
            }
            if ( callback != null )
            {
                callback.onComplete();
            }
        }
    }
}
