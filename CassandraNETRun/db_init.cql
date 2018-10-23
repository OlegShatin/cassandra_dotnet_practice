--- DDL script ---
CREATE KEYSPACE airtickets WITH replication = {'class':'SimpleStrategy','replication_factor':1};
CREATE TABLE flights
                (fl_id int,
                fl_from varchar,
                fl_to varchar,
                fl_cost int,
				PRIMARY KEY(fl_from, fl_to, fl_id)); -- order is important!
CREATE INDEX idx_cost ON flights(fl_cost);

-- samples scripts --
select * from flights where fl_from='BBC' and fl_to = 'CCC';
select count(*) from flights;

drop table flights;

INSERT INTO flights (fl_id, fl_from, fl_to, fl_cost) VALUES (10, 'KZN', 'MSK', 345);

--counters--

CREATE TABLE city_usage
                (cu_city varchar primary key,
                cu_usage counter);

--mat view--
CREATE MATERIALIZED VIEW flights_by_id 
    AS SELECT * FROM flights  
    WHERE fl_id IS NOT NULL 
        AND fl_to IS NOT NULL 
        AND fl_from IS NOT NULL 
    PRIMARY KEY (fl_id, fl_from, fl_to);

    --mat view to test---
    INSERT INTO flights (fl_id, fl_from, fl_to, fl_cost) VALUES (11, 'KZN', 'MSK', 345);
    SELECT * FROM flights_by_id;