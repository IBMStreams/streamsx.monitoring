## SetupApplicationConfiguration

This sample application creates the **applicationConfiguration** for the current domain and instance.

* This application depends on the [streamsx.shell toolkit](https://github.com/IBMStreams/streamsx.shell)
* The required parameters "user", "password", "connectionURL" and "sslOption" are set with this application in the applicationConfiguration.

## Use

Build the application:

`make`

You can specify the toolkit location of the streamx.shell toolkit:

`make STREAMSX_SHELL_TOOLKIT=<path_to_shell_toolkit>`

Run:

In the Streaming Analytics service, click LAUNCH to open the Streams Console, where you can submit and manage your jobs.
Upload the application bundle file ./output/com.ibm.streamsx.metrics.sample.SetupApplicationConfiguration.Main.sab from your file system.
Enter the submission parameters user and password with the "userid" and "password" values from the Service Credentials.

In the Streaming Analytics service, go to the Log Viewer and Click on the PE's Console Log to view output

Clean:

`make clean`


