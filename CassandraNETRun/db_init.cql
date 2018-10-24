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
    SELECT * FROM airtickets.flights_by_id;

--CLUSTER--
0.
Install 'java', 'ccm' 
1.
 //create cluster of 3 nodes of cassandra version 3.11.3 with name airtickets and using vnodes 
ccm create -v 3.11.3 -n 3 airtickets_cluster --vnodes
//list existing clusters
ccm list
ccm start 
ccm status
2.
//requiered 'pip install cqlsh'
$ cqlsh --cqlversion="3.4.4" //connected to node1 - 127.0.0.1
--create our keyspace with replication for each node of cluster (attention to replication_factor)
> CREATE KEYSPACE airtickets WITH replication = {'class':'SimpleStrategy','replication_factor':3};
> USE airtickets;
/*
create tables here! scripts above ^
*/
3. -- check data replication in cluster
--some data insert into node1
> INSERT INTO flights (fl_id, fl_from, fl_to, fl_cost) VALUES (10, 'KZN', 'MSK', 345);
> INSERT INTO flights (fl_id, fl_from, fl_to, fl_cost) VALUES (11, 'KZN', 'SPB', 125);
> exit
$ cqlsh 127.0.0.2 --cqlversion="3.4.4" //connected to node2 - 127.0.0.2
> USE airtickets;
> SELECT * FROM flights;
/*
 fl_from | fl_to | fl_id | fl_cost
---------+-------+-------+---------
     KZN |   MSK |    10 |     345
     KZN |   SPB |    11 |     125

*/
--bingo. Here we are not sure this data indeed stored on node2, but we at least got it.