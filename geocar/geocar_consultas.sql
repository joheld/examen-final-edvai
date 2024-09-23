SELECT *
FROM car_rental_db.car_rental_analytics cra;


SELECT DISTINCT (fueltype)
FROM car_rental_db.car_rental_analytics cra;

--a

SELECT COUNT(*) AS cantidad_alquileres
FROM car_rental_db.car_rental_analytics cra
WHERE  fuelType IN ('HYBRID', 'ELEECTRIC')
  AND rating >= 4;
  
 --b
SELECT state_name, COUNT(*) AS cantidad_alquileres
FROM car_rental_db.car_rental_analytics cra
group by state_name
ORDER BY cantidad_alquileres ASC
limit 5;

--c
SELECT make, model, COUNT(*) AS cantidad_alquileres
FROM car_rental_db.car_rental_analytics cra
group by make, model
ORDER BY cantidad_alquileres DESC 
LIMIT 10;

--d
SELECT `year` , COUNT(*) AS cantidad_alquileres
FROM car_rental_db.car_rental_analytics cra
WHERE `year` BETWEEN 2010 AND 2015
GROUP BY `year` ;

--e

SELECT city, COUNT(*) AS cantidad_alquileres
FROM car_rental_db.car_rental_analytics cra
WHERE fuelType IN ('HYBRID', 'ELECTRIC')
GROUP BY city 
ORDER BY cantidad_alquileres DESC
limit 5;

--f

SELECT fueltype, AVG(reviewcount) as promedio_reviews
FROM car_rental_db.car_rental_analytics cra
group by fueltype;








 