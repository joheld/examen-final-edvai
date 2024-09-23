use aeropuertos_arg;


select *
from aeropuertos_arg.aeropuerto_tabla;

select * from aeropuertos_arg.aeropuerto_detalles_tabla;

--6
SELECT COUNT(*) as cantidad_de_vuelos
from aeropuertos_arg.aeropuerto_tabla
 WHERE 
    fecha BETWEEN '2021-12-01' AND '2022-01-31';


--7

select sum(pasajeros) as cantidad_pasajeros
from aeropuertos_arg.aeropuerto_tabla
WHERE fecha BETWEEN '2021-01-01' AND '2022-06-30';

--8

SELECT 
    fecha,
    horaUTC AS hora,
    CASE 
        WHEN tipo_de_movimiento = 'Despegue' THEN aeropuerto
        ELSE NULL
    END AS codigo_aeropuerto_salida,
    CASE 
        WHEN tipo_de_movimiento = 'Despegue' THEN origen_destino
        ELSE NULL
    END AS ciudad_salida,
    CASE 
        WHEN tipo_de_movimiento = 'Aterrizaje' THEN aeropuerto
        ELSE NULL
    END AS codigo_aeropuerto_arribo,
    CASE 
        WHEN tipo_de_movimiento = 'Aterrizaje' THEN origen_destino
        ELSE NULL
    END AS ciudad_arribo,
    pasajeros
FROM 
    aeropuertos_arg.aeropuerto_tabla
WHERE 
    fecha BETWEEN '2022-01-01' AND '2022-06-30'
ORDER BY 
    fecha DESC;
   
  --9

SELECT 
    aerolinea_nombre,
    SUM(pasajeros) AS total_pasajeros
FROM 
    aeropuertos_arg.aeropuerto_tabla
WHERE 
    fecha BETWEEN '2021-01-01' AND '2022-06-30'
    AND aerolinea_nombre IS NOT NULL
    AND aerolinea_nombre != ''
    AND aerolinea_nombre != '0'
GROUP BY 
    aerolinea_nombre
ORDER BY 
    total_pasajeros DESC
LIMIT 10;

--10

SELECT 
    aeronave,
    COUNT(*) AS cantidad_despegues
FROM 
    aeropuertos_arg.aeropuerto_tabla
WHERE 
    fecha BETWEEN '2021-01-01' AND '2022-06-30'
    AND tipo_de_movimiento = 'Despegue'
    AND origen_destino IN ('EZE', 'AEP', 'EPA', 'FDO', 'LPG')  -- Filtrar por códigos IATA de Buenos Aires
    AND aeronave IS NOT NULL
    AND aeronave != ''
    AND aeronave != '0'
GROUP BY 
    aeronave
ORDER BY 
    cantidad_despegues DESC
LIMIT 10;
