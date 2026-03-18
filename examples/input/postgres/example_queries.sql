use sakila;

/* customer sales summary */
SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  SUM(p.amount) AS total_sales
FROM customer AS c
JOIN payment AS p
  ON c.customer_id = p.customer_id
GROUP BY
  c.customer_id,
  c.first_name,
  c.last_name
ORDER BY
  total_sales DESC NULLS LAST;
/* sales by store */
SELECT
  s.store_id,
  SUM(p.amount) AS total_sales
FROM payment AS p
INNER JOIN rental AS r
  ON p.rental_id = r.rental_id
INNER JOIN inventory AS i
  ON r.inventory_id = i.inventory_id
INNER JOIN store AS s
  ON i.store_id = s.store_id
GROUP BY
  s.store_id
ORDER BY
  total_sales DESC NULLS LAST;
/* top active customers */
SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  COUNT(r.rental_id) AS total_rentals
FROM rental AS r
INNER JOIN customer AS c
  ON r.customer_id = c.customer_id
GROUP BY
  c.customer_id,
  c.first_name,
  c.last_name
ORDER BY
  total_rentals DESC NULLS LAST;
/* customer sales summary */
SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  SUM(p.amount) AS total_sales
FROM customer AS c
JOIN payment AS p
  ON c.customer_id = p.customer_id
GROUP BY
  c.customer_id,
  c.first_name,
  c.last_name
ORDER BY
  total_sales DESC NULLS LAST;
/* most rented films */
SELECT
  f.title,
  COUNT(r.rental_id) AS rental_count
FROM film AS f
JOIN inventory AS i
  ON f.film_id = i.film_id
JOIN rental AS r
  ON i.inventory_id = r.inventory_id
GROUP BY
  f.title
ORDER BY
  rental_count DESC NULLS LAST;
/* inventory by store */
SELECT
  s.store_id,
  s.manager_staff_id,
  COUNT(i.inventory_id) AS total_inventory
FROM store AS s
JOIN inventory AS i
  ON s.store_id = i.store_id
GROUP BY
  s.store_id,
  s.manager_staff_id
ORDER BY
  total_inventory DESC NULLS LAST;
/* sales by category */
SELECT
  c.name AS category,
  SUM(p.amount) AS total_sales
FROM payment AS p
JOIN rental AS r
  ON p.rental_id = r.rental_id
JOIN inventory AS i
  ON r.inventory_id = i.inventory_id
JOIN film AS f
  ON i.film_id = f.film_id
JOIN film_category AS fc
  ON f.film_id = fc.film_id
JOIN category AS c
  ON fc.category_id = c.category_id
GROUP BY
  c.name
ORDER BY
  total_sales DESC NULLS LAST;
/* staff performance for process rentals */
SELECT
  s.staff_id,
  s.first_name,
  s.last_name,
  COUNT(r.rental_id) AS rentals_processed
FROM staff AS s
JOIN rental AS r
  ON s.staff_id = r.staff_id
GROUP BY
  s.staff_id,
  s.first_name,
  s.last_name
ORDER BY
  rentals_processed DESC NULLS LAST;
/* rental delays */
SELECT
  rental_id,
  CAST(EXTRACT(epoch FROM CAST(CAST(return_date AS TIMESTAMP) AS TIMESTAMP) - CAST(CAST(rental_date AS TIMESTAMP) AS TIMESTAMP)) / 86400 AS BIGINT) AS delay_days
FROM rental
WHERE
  return_date > rental_date
