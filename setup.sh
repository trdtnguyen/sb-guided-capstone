INIT_FILE=.airflowinitialized
if [ ! -f $INIT_FILE ]; then
    # Wait until the DB is ready
    #apt update && apt install -y netcat
    #echo "wait postgresql on 5432 to open ..."

    #while ! nc -z postgres_db 5432; do
    #while ! nc -z 0.0.0.0 5432; do
    #    sleep 10
    #done
    #apt remove -y netcat

    # Create all Airflow configuration files
    # Setup the DB for airflow
    #python mysqlconnect.py
    airflow initdb

    # Setup app DB
    python scripts/setup_db.py

    # This configuration is done only the first time
    touch $INIT_FILE
fi

# Run the Airflow webserver and scheduler. Disable this to run manually
#airflow scheduler &
#airflow webserver &
#wait

#Disable this to use airflow scheduler
tail -f /dev/null
