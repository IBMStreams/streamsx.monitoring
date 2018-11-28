# Setup for testing with Streaming Analytics service

## Before launching the test

Ensure that you have Python 3.5 installed. For example, you can get Python 3.5 from the [Anaconda archive page](https://repo.continuum.io/archive/index.html).

Ensure that the bin directory is added to the PATH environment variable. If necessary, add the bin directory by entering the following command on the command line:

    export PATH="~/anaconda3/bin:$PATH"

Ensure that you have set the following environment variables:

* `STREAMING_ANALYTICS_SERVICE_NAME` - name of your Streaming Analytics service
* `VCAP_SERVICES` - [VCAP](https://console.bluemix.net/docs/services/StreamingAnalytics/r_vcap_services.html#r_vcap_services) information in JSON format or a JSON file

Install the latest streamsx package with pip, a package manager for Python, by entering the following command on the command line:

    pip install --user --upgrade streamsx


### Starting a Streaming Analytics service

Make sure that your Streaming Analytics service is running.

* If you have a Streaming Analytics service in IBM Cloud, make sure that it is started and running.
* To create a new Streaming Analytics service:
	* Go to the IBM Cloud web portal and sign in (or sign up for a free IBM Cloud account).
	* Click Catalog, browse for the Streaming Analytics service, and then click it.
	* Enter the service name and then click Create to set up your service. The service dashboard opens and your service starts automatically. The service name appears as the title of the service dashboard.

### Create Application Configuration

Test applications require an application configuration with the name "monitor".
The "monitor" application configuration needs to have a single property with the name "credentials" containing the JSON from the Streaming Analytics service credentials.


# Run the tests
```
ant test
```

# Clean-up

Delete generated files of test suites.
```
ant clean
```
