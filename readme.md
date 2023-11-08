# NYC Taxi Data Analysis Project

This project is designed to analyze New York City taxi data using Groovy scripts within a Gradle project environment. It contains scripts to set up the necessary datasets and perform complex queries to gain insights from the data.

## Prerequisites

Before you begin, ensure you have the following installed:
- Eclipse DSL IDE
- Groovy/Grails Tool Suite
- Java Development Kit (JDK)

## Installation

Follow these steps to get the project up and running:

#### Step 1: Import the Project
Open Eclipse and import the Gradle project. Navigate to `File > Import > Gradle > Existing Gradle Project` and select the project directory.

#### Step 2: Configure Application Properties
Fill up the `application.properties` file located in the resource folder with the necessary configuration settings.

#### Step 3: Initialize the Dataset
Run `SetupData.groovy` as a Groovy script to set up the initial dataset required for analysis.
> **NOTE:** The dataset provided is pre-populated with necessary field values for demonstration purposes. It is recommended not to fetch new data during the demo as it may be time-consuming and require significant disk space.

#### Step 4: Run Analysis Scripts
Execute either `Groovy_Query.groovy` or `MongoDB_Query.groovy` as Groovy scripts to perform data analysis.

## Scripts Overview
* `Groovy_Query.groovy`: Performs data analysis by executing complex queries directly on the dataset using Groovy's capabilities.
* `MongoDB_Query.groovy`: Leverages MongoDB's powerful querying capabilities to analyze the data and provide insights.
* `SetupData.groovy`: Prepares the dataset necessary for running the analysis scripts.
* `JsonHelper.groovy` and `MongoDBHelper.groovy`: Serve as helper classes to facilitate operations on JSON data and interaction with MongoDB, respectively.

## Data Description
The NYC taxi data being analyzed includes the following fields:
* `trip_miles`: The total miles of the trip.
* `trip_time`: The duration of the trip.
* `base_passenger_fare`: The basic fare paid by the passenger.
* `tips`: Tips given to the driver.
* `driver_pay`: The amount paid to the driver.
* `hvfhs_license_num`: The license number of the High Volume For-Hire Service.
* `dispatching_base_num`: The base number from which the vehicle is dispatched.

For an in-depth understanding of the dataset, refer to the [NYC Taxi Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhs.pdf)<br>
The dataset can be obtained from the [NYC Taxi & Limousine Commission Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Performance Optimization
When working with extensive datasets in the NYC Taxi Data Analysis Project, it may be necessary to increase the Java Virtual Machine (JVM) heap size to improve performance and avoid out-of-memory issues.

#### Increasing Heap Memory in Eclipse
To allocate more heap memory in Eclipse:
1. Navigate to `Run -> Run Configurations`.
2. Within the Run Configurations window, select the specific run configuration for your Groovy script.
3. Switch to the `Arguments` tab.
4. In the `VM arguments` section, enter the following options:
```
-Xms512m
-Xmx2048m
```
Here, `-Xms512m` sets the initial heap size, and `-Xmx2048m` sets the maximum heap size. Adjust the numbers to suit your data size and system capabilities. Make sure to apply the changes and close the Run Configurations window before executing your script.<br>
If this does not work out please follow [Eclipse OutOfMemory Error Fix ](https://www.digitalocean.com/community/tutorials/eclipse-out-of-memory-error-increasing-heap-memory-permgen-space)

## Acknowledgments
New York City Taxi and Limousine Commission for providing the dataset.<br>
Contributors and maintainers of this project.

&copy; Dhrupal Shah, 2023-2024
