
# Arrow Flight Dataset
This module implements the Arrow Flight RPC as a Kedro Dataset in the DremioFlightDataSet class.

The `DremioFlightDataSet` class inherits from the `AbstractDataSet` class from the Kedro IO library. This means that it has the following methods:

-   `_load()`: Loads the data from the dataset.
-   `_save()`: Saves the data to the dataset.
-   `_exists()`: Checks whether the data exists in the dataset.
-   `_describe()`: Describes the data in the dataset.
-   `close()`: Closes the dataset.

The `DremioFlightDataSet` class also has the following properties:

-   `_client`: The FlightClient object that is used to connect to the Dremio Flight server.
-   `_load_args`: The arguments that are used to load the data from the dataset.
-   `_filepath`: The filepath of the dataset.
-   `_flight_con`: The connection configuration for the Dremio Flight server.

To use the `DremioFlightDataSet` class, you first need to create an instance of the class. You can do this by passing the following arguments to the constructor:

-   `sql`: The SQL query that you want to run against the Dremio Flight server.
-   `credentials`: The credentials that you use to connect to the Dremio Flight server.
-   `load_args`: The arguments that you want to use to load the data from the dataset.
-   `fs_args`: The arguments that you want to use to access the filesystem where the dataset is stored.
-   `filepath`: The filepath of the dataset.

Once you have created an instance of the `DremioFlightDataSet` class, you can use the `_load()` method to load the data from the dataset. The `_load()` method returns a `DataFrame` object or a `FlightStreamReader` object. The `DataFrame` object contains the data that was loaded from the dataset. The `FlightStreamReader` object is a stream that you can use to read the data from the dataset.

You can also use the `_save()` method to save the data to the dataset. The `_save()` method takes a `DataFrame` object as input and saves the data to the dataset.

You can use the `_exists()` method to check whether the data exists in the dataset. The `_exists()` method returns a boolean value. The boolean value is `True` if the data exists in the dataset and `False` if the data does not exist in the dataset.

You can use the `_describe()` method to describe the data in the dataset. The `_describe()` method returns a `FlightInfo` object. The `FlightInfo` object contains information about the data in the dataset, such as the schema of the data, the number of rows in the data, and the size of the data.

Finally, you can use the `close()` method to close the dataset. The `close()` method closes the connection to the Dremio Flight server and the filesystem where the dataset is stored.
