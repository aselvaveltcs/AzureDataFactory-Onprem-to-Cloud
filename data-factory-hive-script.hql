set hive.exec.dynamic.partition.mode = nonstrict;

DROP TABLE IF EXISTS customer; 
CREATE  TABLE customer (
  customer_id  int,
  first_name  string,
  last_name string,
  address_id  int 
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'  ;

LOAD DATA INPATH '${hiveconf:inputcustomertable}' OVERWRITE INTO TABLE customer;


DROP TABLE IF EXISTS film; 
CREATE EXTERNAL TABLE film (
  film_id  int,
  title  string,
  rental_duration_in_days int 
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' ;

LOAD DATA INPATH '${hiveconf:inputfilmtable}' OVERWRITE INTO TABLE film;
 


DROP TABLE IF EXISTS inventory; 
CREATE EXTERNAL TABLE inventory (
  inventory_id  int,
  film_id int 
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' ;

LOAD DATA INPATH  '${hiveconf:inputinventorytable}' OVERWRITE INTO TABLE inventory;

 
DROP TABLE IF EXISTS rental; 
CREATE EXTERNAL TABLE rental (
  rental_id  int,
  inventory_id int,
  customer_id int,
  rental_date date  
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';

LOAD DATA INPATH  '${hiveconf:inputrentaltable}' OVERWRITE INTO TABLE rental;


DROP TABLE IF EXISTS address; 
CREATE EXTERNAL TABLE address (
  address_id  int,
  district string,
  phone string  
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' ;

 LOAD DATA INPATH  '${hiveconf:inputaddresstable}' OVERWRITE INTO TABLE address;


DROP TABLE IF EXISTS overdue_dvds; 
CREATE TABLE overdue_dvds AS
SELECT CONCAT(customer.first_name ,' ',customer.last_name) AS customer_name,
    address.phone AS customer_phone, address.district AS customer_location, film.title AS rented_film_title	
    FROM rental JOIN customer ON (rental.customer_id = customer.customer_id)
    JOIN address ON (customer.address_id = address.address_id)
    JOIN inventory ON (rental.inventory_id = inventory.inventory_id)
    JOIN film ON (inventory.film_id = film.film_id)
    WHERE date_add(rental.rental_date, film.rental_duration_in_days) < current_date();
	
-- EXPORT TABLE overdue_dvds TO '${hiveconf:outputoverduetable}';
	
INSERT OVERWRITE DIRECTORY '${hiveconf:outputoverduetable}'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
select * from overdue_dvds;