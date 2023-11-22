# env_file="dev_env"
env_file="deploy_env"
network_name="q_data_proj_pg_net"

if docker network inspect "$network_name" >/dev/null 2>&1; then
    echo "The network $network_name exists."
else
    echo "The network $network_name does not exist, creating it!"
    docker network create $network_name
fi

echo "up db...."
docker-compose -f db_compose.yml -p db --env-file ${env_file}  up -d
echo "db running...."
echo "stop command: docker-compose -f db_compose.yml -p db down"

echo "up tools..."
# FIXME this name should be fixed
docker exec db-postgres-1 psql -d postgres -c "CREATE DATABASE prefect"

docker-compose -f tools_compose.yml --env-file ${env_file} -p tools up -d
echo "tools running..."
echo "stop command: docker-compose -f tools_compose.yml --env-file ${env_file} up"
