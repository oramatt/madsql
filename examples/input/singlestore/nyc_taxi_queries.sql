use nyc_taxi;

-- Total number of trips for each neighborhood
SELECT COUNT(*) num_rides, n.name
FROM trips t, neighborhoods n
WHERE
    n.id IN (
    	SELECT id FROM neighborhoods
    ) AND
    GEOGRAPHY_INTERSECTS(t.pickup_location, n.polygon)
GROUP BY n.name
ORDER BY num_rides DESC;

-- The average amount of time between someone requesting a ride and that person being picked up
SELECT ROUND(AVG(pickup_time - request_time) / 60,2) val
FROM trips t, neighborhoods n
WHERE
    n.id IN (
        SELECT id FROM neighborhoods
    ) AND
    GEOGRAPHY_INTERSECTS(t.pickup_location, n.polygon) AND
    pickup_time != 0 AND
    request_time != 0;

-- The average distance of a trip
SELECT ROUND(AVG(geography_distance(pickup_location, dropoff_location) / 1000), 2) val
FROM trips t, neighborhoods n
WHERE
    n.id IN (
        SELECT id FROM neighborhoods
    ) AND
    GEOGRAPHY_INTERSECTS(t.pickup_location, n.polygon);

-- The average amount of time between someone being picked up and that person being dropped off
SELECT ROUND(AVG(dropoff_time - pickup_time) / 60, 2) val
FROM trips t, neighborhoods n
WHERE
    status = "completed" AND
    n.id IN (
        SELECT id FROM neighborhoods
    ) AND
    GEOGRAPHY_INTERSECTS(t.pickup_location, n.polygon);

-- The average cost of a trip
SELECT ROUND(AVG(price), 2) val
FROM trips t, neighborhoods n
WHERE
    status = "completed" AND
    n.id IN (
        SELECT id FROM neighborhoods
    ) AND
    GEOGRAPHY_INTERSECTS(t.pickup_location, n.polygon);

-- The average amount of time it takes from the time a driver accepts a ride to the time they pick up the passenger
SELECT ROUND(AVG(pickup_time - accept_time) / 60, 2) val
FROM trips t, neighborhoods n
WHERE
    pickup_time != 0 AND
    accept_time != 0 AND
    n.id IN (
        SELECT id FROM neighborhoods
    ) AND
    GEOGRAPHY_INTERSECTS(t.pickup_location, n.polygon);

-- The average number of riders per trip
SELECT ROUND(AVG(num_riders), 2) val
FROM trips t, neighborhoods n
WHERE
    status = "completed" AND
    n.id IN (
        SELECT id FROM neighborhoods
    ) AND
    GEOGRAPHY_INTERSECTS(t.pickup_location, n.polygon);