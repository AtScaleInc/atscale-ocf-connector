# AtScale Alation OCF Connector

This project contains the code to create an Alation OCF Connector to AtScale. Use the following instructions to build and package this code into a zip file for installing in Alation.

## Development Environment Setup
1. Download the Alation OCF SDK Jars
2. From the console, run: `mvn install:install-file -Dfile=sdk-4.1.24-jar-with-dependencies.jar -DgroupId=rdbms -DartifactId=sdk -Dversion=4.1.24 -Dpackaging=jar`
3. From the console, run: `mvn install:install-file -Dfile=sdk_tests-3.2.18-jar-with-dependencies.jar -DgroupId=rdbms -DartifactId=sdk_tests -Dversion=3.2.18 -Dpackaging=jar`
4. If using Intellij, create the following configurations:
   1. Name: `Start Server`, Main Class: `atscale.biconnector.Main`, Parameters: `START_SERVER`
   2. Name: `Run Extraction`, Main Class: `alation.sdk.bi.grpc.client.AbstractBIClient`, Parameters: `-p 8980 --conf configuration.json -o results FULL_EXTRACTION`
   3. Name: `Validate Configuration`, Main Class: `alation.sdk.bi.grpc.client.AbstractBIClient`, Parameters: `-p 8980 --conf configuration.json -o results CONFIGURATION_VERIFICATION`

## Packaging Instructions
1. In IntelliJ, Execute Maven Goal: `mvn clean`
2. In IntelliJ, Execute Maven Goal: `mvn compile`
3. In IntelliJ, Execute Maven Goal: `mvn assembly:single`
4. From the console in the project directory, run `./package_connector.sh -m biconnector -j target/biconnector-1.1.0-jar-with-dependencies.jar -f Dockerfile`
5. If successful, it will produce a file named `atscale-alation-connector-x.x.x.zip`
