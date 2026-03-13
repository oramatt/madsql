-- customer sales summary
SELECT c.customer_id, c.first_name, c.last_name, SUM(p.amount) AS total_sales
FROM customer c
JOIN payment p ON c.customer_id = p.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY total_sales DESC;

-- sales by store
SELECT s.store_id, SUM(p.amount) AS total_sales
FROM payment p
INNER JOIN rental r ON p.rental_id = r.rental_id
INNER JOIN inventory i ON r.inventory_id = i.inventory_id
INNER JOIN store s ON i.store_id = s.store_id
GROUP BY s.store_id
ORDER BY total_sales DESC;


-- top active customers
SELECT c.customer_id, c.first_name, c.last_name, COUNT(r.rental_id) AS total_rentals
FROM rental r
INNER JOIN customer c ON r.customer_id = c.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY total_rentals DESC;


-- customer sales summary
SELECT c.customer_id, c.first_name, c.last_name, SUM(p.amount) AS total_sales
FROM customer c
JOIN payment p ON c.customer_id = p.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY total_sales DESC;


-- most rented films
SELECT f.title, COUNT(r.rental_id) AS rental_count
FROM film f
JOIN inventory i ON f.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
GROUP BY f.title
ORDER BY rental_count DESC;


-- inventory by store
SELECT s.store_id, s.manager_staff_id, COUNT(i.inventory_id) AS total_inventory
FROM store s
JOIN inventory i ON s.store_id = i.store_id
GROUP BY s.store_id, s.manager_staff_id
ORDER BY total_inventory DESC;


-- sales by category
SELECT c.name AS category, SUM(p.amount) AS total_sales
FROM payment p
JOIN rental r ON p.rental_id = r.rental_id
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN film f ON i.film_id = f.film_id
JOIN film_category fc ON f.film_id = fc.film_id
JOIN category c ON fc.category_id = c.category_id
GROUP BY c.name
ORDER BY total_sales DESC;


-- staff performance for process rentals
SELECT s.staff_id, s.first_name, s.last_name, COUNT(r.rental_id) AS rentals_processed
FROM staff s
JOIN rental r ON s.staff_id = r.staff_id
GROUP BY s.staff_id, s.first_name, s.last_name
ORDER BY rentals_processed DESC;


-- rental delays
SELECT rental_id, DATEDIFF(DAY, rental_date, return_date) AS delay_days
FROM rental
WHERE return_date > rental_date;





