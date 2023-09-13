#!/usr/bin/env bash

MODULE=''
MODULE_JAR_FILE=''
DOCKER_FILE=''
DATA_SOURCE_TYPE=''
DRIVER_JAR=''

REL_SCRIPT_DIR=$( dirname "$BASH_SOURCE" )
SCRIPT_DIR=$( cd "${REL_SCRIPT_DIR}" && pwd )

print_usage() {
  echo "Usage: ./package_connector.sh -m <module_name> -j <module_jar_file_path> -f <docker_file_path> -t <data_source_type> -d <jdbc_driver_jar>"
  echo "Provide module jar file path using -j and docker file path with -f."
  echo "Provide the value for data source type. Accepted values are {rdbms, bi}."
  echo "Provide JDBC driver jar file path using -d if the connector supports Compose. This is applicable only for RDBMS data sources."
}

while getopts 'm:j:f:t:d:' flag; do
  case "$flag" in
    m) MODULE="${OPTARG}" ;;
    j) MODULE_JAR_FILE="${OPTARG}" ;;
    f) DOCKER_FILE="${OPTARG}" ;;
    t) DATA_SOURCE_TYPE="${OPTARG}" ;;
    d) DRIVER_JAR="${OPTARG}" ;;
    *) print_usage
       exit 1 ;;
  esac
done

echo "Packaging module: ${MODULE}"
echo " Step 1: Validate params to package the connector "
if [[ ${MODULE} == '' ]]
  then
  echo "Module name cannot be blank."
  exit 1 ;
fi
if [[ ${MODULE_JAR_FILE} == '' ]]
  then
  echo "Module driver jar path cannot be blank."
  exit 1 ;
fi
if [[ ${DOCKER_FILE} == '' ]]
 then
 echo "Docker file path cannot be blank."
 exit 1 ;
fi

echo " Step 2: Making MANIFEST.MF..."
java -cp ${MODULE_JAR_FILE} atscale.${MODULE}.Main MAKE_MANIFEST


echo " Step 3: JDBC driver packaging for supported modules"
if [[ ${DRIVER_JAR} != '' && ${DATA_SOURCE_TYPE} == 'rdbms' ]]
then
    echo "Packaging JDBC driver for the module..."
    cp ${DRIVER_JAR} .
    # Split and get driver name from the path variable
    OLD_IFS=${IFS}
    IFS='/ '
    read -ra path_split <<<${DRIVER_JAR}
    driverName=${path_split[${#path_split[@]}-1]}
    IFS=${OLD_IFS}
 else
    echo "Skipping JDBC driver packaging for the module."
 fi

connectorName="atscale-${MODULE}-ocf-connector"

echo " Step 4: Building docker image..."
docker build --add-host ptoole-dev.docker.infra.atscale.com:10.200.2.100 --platform linux/amd64 -t ${connectorName} --build-arg module_jar_file=${MODULE_JAR_FILE} -f $DOCKER_FILE .

echo " Step 5: Saving docker image..."
docker save -o ${connectorName}.img ${connectorName}

echo " Step 6: Removing docker image from the host node..."
docker rmi ${connectorName}

echo " Step 7: Zipping connector..."
zip ${connectorName}.zip ${connectorName}.img ${driverName} MANIFEST.MF

echo " Step 8: Removing temp files..."
rm -f ${connectorName}.img ${driverName} MANIFEST.MF

echo " Step 9: Renaming zip file..."
mv ${connectorName}.zip "atscale-alation-connector-1.1.0.zip"

echo "Packaging module ${MODULE} completed."
