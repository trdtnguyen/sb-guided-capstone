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
