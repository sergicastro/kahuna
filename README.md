Abiquo API Command Line Tools
=============================

This project is a simple command line tool for the Abiquo Cloud Platform API, using
Jython as a wrapper to the [Official Java client](https://github.com/abiquo/jclouds-abiquo).
To understand the code and adapt it to your needs, you may want to take a
look at the official Java client project page:

 * [jclouds-abiquo Source code](https://github.com/abiquo/jclouds-abiquo)
 * [jclouds-abiquo Documentation](https://github.com/abiquo/jclouds-abiquo/wiki)

You can also find some usage examples in the [Project Wiki](https://github.com/nacx/kahuna/wiki).


Prerequisites
-------------

To run the examples you will need to install *Maven >= 2.2.1*, *JRE >= 1.6*
and ***Jython >= 2.5.2***.


Configuration
-------------

Since this project is currently in development process and we don't have any egg 
package ready, the *$JYHTONPATH* environment variable needs to be set manually:

    export JYTHONPATH=$(YOUR_PROJECT_HOME_DIRECTORY)


Running
-------

You can run the command line using the **kahuna.sh** script. If you run it without
parameters, it will print the help with all the available options:

    $ sh kahuna.sh
    
    Usage: kahuna.sh <plugin> <command> [<options>]
    The following plugins are available:
    
    Environment generator plugin usage:
      env create     Creates the environment. 
      env clean      Cleans up the environment. 
    
    Interactive shell plugin usage:
      shell open     Opens an interactive shell. 
    
    Virtual machine plugin usage:
      vm find        Find a virtual machine given its name. 
      vm list        List all virtual machines.

Use it interactively
--------------------

You can also perform actions against the Api in an interactive way. You just need to
use the 'shell' plugin as follows:

    $ sh kahuna.sh shell open
    Jython 2.5.2 (Release_2_5_2:7206, Mar 2 2011, 23:12:06) 
    [Java HotSpot(TM) Server VM (Sun Microsystems Inc.)] on java1.6.0_18
    Type "help", "copyright", "credits" or "license" for more information.
    >>> from kahuna.session import ContextLoader
    >>> ctx = ContextLoader().load_context()
    Using endpoint: http://10.60.1.222/api
    >>> cloud = ctx.getCloudService()         
    >>> cloud.listVirtualDatacenters()
    [VirtualDatacenter [id=11, type=XENSERVER, name=Kaahumanu]]
    >>> vdc = cloud.listVirtualDatacenters()[0]
    >>> vdc.setName("Updated VDC")
    >>> vdc.update()
    >>> cloud.listVirtualDatacenters()         
    [VirtualDatacenter [id=11, type=XENSERVER, name=Updated VDC]]
    >>> ctx.close()
    >>> exit()


Note on patches/pull requests
-----------------------------
 
 * Fork the project.
 * Create a topic branch for your feature or bug fix.
 * Develop in the just created feature/bug branch.
 * Add tests for it. This is important so I don't break it in a future version unintentionally.
 * Commit.
 * Send me a pull request.


Issue Tracking
--------------

If you find any issue, please submit it to the [Bug tracking system](https://github.com/nacx/kahuna/issues) and we
will do our best to fix it.

License
-------

This sowftare is licensed under the MIT license. See LICENSE file for details.

