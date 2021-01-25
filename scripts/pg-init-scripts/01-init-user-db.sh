#!/bin/bash
# set -e means exit immediately if a command exits with a non-zero status
set -e
echo "Test psql. Wait for posgresql ready"
pg_isready -U postgres -h 0.0.0.0 -p 5432
#apt update && apt install -y netcat
#while ! nc -z postgres_db 5432; do
#    sleep 10
#done

apt remove -y netcat

psql -U postgres -d sbstock -c 'SELECT 1;'

psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS job_tracker;

    CREATE TABLE IF NOT EXISTS job_tracker(
        job_id bigint NOT NULL,
        job_name VARCHAR(128) NOT NULL,
        status VARCHAR(64) NOT NULL,
        updated_time timestamp NOT NULL,
        PRIMARY KEY (job_id)
    );

    INSERT INTO job_tracker  VALUES (1, 'job1', 'ok', '2021-01-21 12:00:00');
    INSERT INTO job_tracker  VALUES (2, 'job2', 'ok', '2021-01-21 12:10:00');
    INSERT INTO job_tracker  VALUES (3, 'job3', 'ok', '2021-01-21 12:30:00');
EOSQL
