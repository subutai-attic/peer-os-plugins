package io.subutai.plugin.etl.ui;


import java.util.List;

import com.vaadin.server.Sizeable;
import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.AbstractTextField;
import com.vaadin.ui.Button;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.Label;
import com.vaadin.ui.PasswordField;
import com.vaadin.ui.ProgressBar;
import com.vaadin.ui.TextArea;
import com.vaadin.ui.TextField;

import io.subutai.plugin.sqoop.api.Sqoop;
import io.subutai.plugin.sqoop.api.SqoopConfig;


public class UIUtil
{

    public static Button getButton( String caption, Button.ClickListener listener )
    {
        Button button = new Button( caption );
        button.addStyleName( "default" );
        if ( listener != null )
        {
            button.addClickListener( listener );
        }
        return button;
    }


    public static ProgressBar getProgressIcon()
    {
        ProgressBar progressIcon = new com.vaadin.ui.ProgressBar();
        progressIcon.setId( "indicator" );
        progressIcon.setIndeterminate( true );
        progressIcon.setVisible( false );
        return progressIcon;
    }


    public static ComboBox getComboBox( String caption )
    {
        ComboBox comboBox = new ComboBox();
        comboBox.setCaption( caption );
        comboBox.setNullSelectionAllowed( false );
        comboBox.setImmediate( true );
        comboBox.setTextInputAllowed( false );
        comboBox.setRequired( true );
        return comboBox;
    }


    public static TextArea getTextArea( String caption )
    {
        TextArea textArea = new TextArea( caption );
        textArea.setSizeFull();
        textArea.setRows( 30 );
        textArea.setWordwrap( false );
        return textArea;
    }


    public static Label getLabel( String text, float width )
    {
        return getLabel( text, Sizeable.Unit.PIXELS );
    }


    public static Label getLabel( String text, Sizeable.Unit sizeAble )
    {
        Label label = new Label( text );
        label.setSizeFull();
        label.setContentMode( ContentMode.HTML );
        return label;
    }


    public static AbstractTextField getTextField( String label )
    {
        return getTextField( label, false );
    }


    public static AbstractTextField getTextField( String label, boolean isPassword )
    {
        AbstractTextField textField = isPassword ? new PasswordField( label ) : new TextField( label );
        textField.setSizeFull();
        return textField;
    }


    public static String findSqoopClusterName( Sqoop sqoop, String hostId )
    {
        List<SqoopConfig> sqoopConfigList = sqoop.getClusters();
        for ( SqoopConfig config : sqoopConfigList )
        {
            if ( config.getNodes().contains( hostId ) )
            {
                return config.getClusterName();
            }
        }
        return null;
    }
}
