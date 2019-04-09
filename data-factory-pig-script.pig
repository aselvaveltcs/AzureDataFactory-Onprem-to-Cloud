CUSTOMER = LOAD '$InputCustomer' USING PigStorage(',');
FILM = LOAD '$InputFilm' USING PigStorage(',');
INVENTORY = LOAD '$InputInventory' USING PigStorage(',');
RENTAL = LOAD '$InputRental' USING PigStorage(',');
ADDRESS = LOAD '$InputAddress' USING PigStorage(',');


CUSTOMER_DATA = FOREACH CUSTOMER GENERATE  (int) $0 AS customer_id, (chararray) $2 AS first_name, (chararray) $3 AS last_name, (int) $5 AS address_id ;
CUSTOMER_DATA_UNQ = DISTINCT CUSTOMER_DATA;
Store CUSTOMER_DATA_UNQ into '$OutputCustomer' USING PigStorage (','); 


FILM_DATA = FOREACH FILM GENERATE  (int) $0 AS film_id, (chararray) $1 AS title, (int) $6 AS rental_duration  ;
FILM_DATA_UNQ = DISTINCT FILM_DATA;
Store FILM_DATA_UNQ into '$OutputFilm' USING PigStorage (','); 


INVENTORY_DATA = FOREACH INVENTORY GENERATE  (int) $0 AS inventory_id, (int) $1 AS film_id   ;
INVENTORY_DATA_UNQ = DISTINCT INVENTORY_DATA;
Store INVENTORY_DATA_UNQ into '$OutputInventory' USING PigStorage (','); 


RENTAL_DATA = FOREACH RENTAL GENERATE  (int) $0 AS rental_id, (int) $2 AS inventory_id , (int) $3 AS customer_id,  (chararray) $1 AS rental_date, (chararray) $4 AS return_date  ;
RENTAL_DATA_UNQ = DISTINCT RENTAL_DATA;
RENTAL_DATA_FILTER = FILTER RENTAL_DATA_UNQ BY return_date == 'NO_DATA'  ;
RENTAL_DATA_UNQ = FOREACH RENTAL_DATA_FILTER  GENERATE rental_id, inventory_id, customer_id,  ToDate(SUBSTRING(REPLACE(rental_date, 'NO_DATA',''),1,10),'YYYY-MM-DD') AS rental_date; 
Store RENTAL_DATA_UNQ into '$OutputRental' USING PigStorage (','); 


ADDRESS_DATA = FOREACH ADDRESS GENERATE  (int) $0 AS address_id, (chararray) $3 AS district, (chararray) $6 AS phone   ;
ADDRESS_DATA_UNQ = DISTINCT ADDRESS_DATA;
Store ADDRESS_DATA_UNQ into '$OutputAddress' USING PigStorage (','); 
