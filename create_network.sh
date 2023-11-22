env_file="dev_env"
#env_file="deploy_env"
network_name="your_network_name"

if docker network inspect "$network_name" >/dev/null 2>&1; then
    echo "The network $network_name exists."
else
    echo "The network $network_name does not exist, creating it!"
fi
docker network create $network_name

echo "up db...."
docker-compose -f db_compose.yml -p db --env-file ${env_file} up
echo "db running...."
echo "stop command: docker-compose -f db_compose.yml -p db down"

echo "up tools..."
docker exec db-postgres-1 psql -d postgres -c "CREATE DATABASE prefect"

docker-compose -f tools_compose.yml --env-file ${env_file} up
echo "tools running..."
echo "stop command: docker-compose -f tools_compose.yml --env-file ${env_file} up"
