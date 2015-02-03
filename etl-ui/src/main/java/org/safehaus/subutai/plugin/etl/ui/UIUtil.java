package org.safehaus.subutai.plugin.etl.ui;


import com.vaadin.server.Sizeable;
import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.AbstractTextField;
import com.vaadin.ui.Button;
import com.vaadin.ui.Label;
import com.vaadin.ui.PasswordField;
import com.vaadin.ui.TextArea;
import com.vaadin.ui.TextField;


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
}
