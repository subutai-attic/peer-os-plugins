# Subutai Console Plugins repository

This repository contains source code of plugins for Subutai Console.
This is a multi-module Maven Java project.

These plugins are available in Subutai Console's Plugins section and can be installed directly from there.

## Building the project

###Prerequisites

- Latest Subutai Console

  [Installation](https://github.com/subutai-io/peer-os)
  
###Build steps

- Clone the project by using:

    `git clone https://github.com/subutai-io/peer-os-plugins.git`

- Start maven build:

    ```bash
    cd plugins
    mvn clean install
    ```
    
- You can pull all kar files into `kar` folder by executing script

    ```bash
    bash karPuller.sh
    ```
- Deploy file using Plugins's Advanced tab.

    - Go to Plugins -> Advanced on your Subutai Console and press "Upload new plugin"
    
    ![alt tag](http://i.imgur.com/h1ebTfm.png)
    
    - Fill all fields and upload
    
    ![alt tag](http://i.imgur.com/bEYjMCb.png)
