/*
Nishant Tewari 
190684430
tewa4430@mylaurier.ca
Wednesday November 15th, 2023 
*/

data = LOAD 'AB_NYC_2019.csv' USING PigStorage(',')
     AS (id:int, name:chararray, host_id:int, host_name:chararray,
         neighbourhood_group:chararray, neighbourhood:chararray,
         latitude:float, longitude:float, room_type:chararray,
         price:int, minimum_nights:int, number_of_reviews:int,
         last_review:chararray, reviews_per_month:double,
         calculated_host_listings_count:int, availability_365:int);

-- Step 1: Filter the data by minimum_nights > 10, number_of_reviews > 10 and the last_review falls within 2018 or 2019.
filtered_data = FILTER data  BY (minimum_nights > 10) AND (number_of_reviews > 10) AND ((last_review MATCHES '.*2019.*') OR (last_review MATCHES '.*2018.*'));

-- Step 2: Group the neighbourhood information by average price and availability 
calculate = FOREACH (GROUP filtered_data  BY neighbourhood_group) GENERATE group, AVG(filtered_data.price) AS average_price, AVG(filtered_data.availability_365) AS average_avail;

-- Rearrange by price 
order_by_price = ORDER calculate BY average_price DESC;

-- Store them to a folder called 'AirBnB_neighbourhood'
STORE order_by_price INTO 'AirBnB_neighbourhood' USING PigStorage(',');
--Show the final output 
DUMP order_by_price;

--Step 3: Display the room type, lowest price of room, & name 

group_rooms = GROUP filtered_data BY room_type;
result = FOREACH group_rooms {
    rooms_ordered = ORDER filtered_data BY price ASC;
    lowest = LIMIT rooms_ordered 1;
    GENERATE group AS room_type, 
             flatten(lowest.price) AS lowest_price, 
             flatten(lowest.name) AS property_name;
}
-- Show the final output 
DUMP result;