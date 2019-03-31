
--Create keyspace in cassandra
CREATE KEYSPACE test
    WITH REPLICATION = {
        'class': 'SimpleStrategy',
        'replication_factor': 1
    };


--Create trend table with counter type
CREATE TABLE hashtrend2 (
     hashname TEXT PRIMARY KEY,
     popularity COUNTER
 );

--Insert into table without counter
 INSERT INTO hashtrend (hashname,popularity) VALUES ('NCAA', 2);

--Insert into table without counter
 INSERT INTO countries (id, official_name, capital_city) VALUES (1, 'Islamic Republic of Afghanistan', 'Kabul');

--Insert into table with counter
 update weblogs set page_count = page_count + 1
where page_id =uuid() and page_name =’test.com’ and insertion_time =dateof(now());

--Insert into table with counter
update hashtrend set popularity = popularity + 2 where hashname ='NCAA';