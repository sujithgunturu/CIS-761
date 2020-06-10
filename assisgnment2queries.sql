/*1)*/
SELECT FC1.continent, COUNT(FC1.continent) as Numberofairports from fcountries FC1 join airports A1
on FC1.code = A1.country 
GROUP BY FC1.continent;



/*2)*/
select airport, sum(departure) departure, sum(arrival) arrival
from (select departure airport, 1 departure, 0 arrival from flights
union all
select arrival airport, 0 departure, 1 arrival from flights
) T
group by airport
order by airport;



/*3)*/
SELECT flightnum from flights
WHERE departure ='BOS' AND arrival ='ORD' AND airline = (SELECT CODE from airlines where name ='American Airlines');


/*4)*/
SELECT A.name from flights F,  airlines A WHERE F.airline = A.code
GROUP by airline 
HAVING count(departure) > 100;


/*5)*/
SELECT name,city, state from airports WHERE code IN(
 SELECT T1.arr1 from 
(SELECT DISTINCT(F1.arrival )as arr1  from 
flights F1 join airports A1 on F1.departure = A1.code join airports A2 ON F1.arrival = A2.code 
WHERE A1.country = 'us' and A2.country ='us' AND A1.state != 'NY' AND A2.state !='NY')T1
LEFT JOIN
(SELECT DISTINCT(F1.arrival ) as arr2  from 
flights F1 join airports A1 on F1.departure = A1.code join airports A2 ON F1.arrival = A2.code 
WHERE A1.country = 'us' and A2.country ='us' AND F1.departure ='JFK')T2
ON T1.arr1 = T2.arr2
WHERE T2.arr2 IS NULL);



/*6)*/
SELECT A.name, COUNT(departure) from flights F,  airlines A WHERE F.airline = A.code 
GROUP by airline 
HAVING count(departure)  = (SELECT MAX(C) FROM                   
(SELECT COUNT(departure) AS C from flights F, airlines A WHERE F.airline = A.code 
GROUP by airline ) T);


/*7)*/
SELECT Ar.name, F1.flightnum,F1.departure,F1.arrival,F1.duration,F1.dep_time, F2.flightnum, F2.departure,F2.arrival, F2.dep_time, F2.duration, 
 MINUTE(TIMEDIFF(
     AddTime(F2.dep_time, SEC_TO_TIME(A1.utc_offset*60*60))
     ,AddTime(AddTime(F1.dep_time, SEC_TO_TIME(F1.duration*60)), SEC_TO_TIME(A1.utc_offset*60*60))
 )) + F1.duration + F2.duration as total 
from flights F1 INNER join flights F2 on F2.departure = F1.arrival JOIN airports A1 on F1.arrival = A1.code JOIN airlines Ar on Ar.code = F1.airline
WHERE  F1.departure = 'BOS' AND F1.arrival!='DFW' AND F2.arrival ='DFW' AND F1.airline = F2.airline AND 
AddTime(F2.dep_time, SEC_TO_TIME(A1.utc_offset*60*60)) > AddTime(AddTime(F1.dep_time, SEC_TO_TIME(F1.duration*60)), SEC_TO_TIME(A1.utc_offset*60*60))
HAVING total < 480;


/*8)*/
SELECT DISTINCT(A2.city)
from flights F1 INNER join flights F2 on F2.departure = F1.arrival JOIN airports A1 on F1.arrival = A1.code JOIN airports A2 ON F2.arrival = A2.code
WHERE  F1.departure = 'BUD' AND F2.arrival IN 
(
    SELECT T1.firstone 
from (SELECT DISTINCT(arrival) AS firstone from flights)T1 LEFT JOIN  (SELECT DISTINCT(arrival) AS secondone FROM flights where departure ='BUD')T2 
ON T1.firstone = T2.secondone where T2.secondone IS NULL
)
AND AddTime(F2.dep_time, SEC_TO_TIME(A1.utc_offset*60*60)) > AddTime(AddTime(F1.dep_time, SEC_TO_TIME(F1.duration*60)), SEC_TO_TIME(A1.utc_offset*60*60))
ORDER BY A2.city; 


/*9)*/
SELECT A.city FROM flights F, airports A 
where  DURATION/ 60 <=4 and F.departure = A.code
GROUP by city
ORDER by city;



/*10*/
SELECT A1.city, A2.city, max(duration) As maximumDuration
from flights F1 JOIN airports A1 ON F1.departure = A1.code JOIN airports A2 on F1.arrival =A2.code
where (departure, duration) IN (SELECT departure, MAX(duration) from 
flights 
GROUP BY departure)
GROUP BY departure, arrival
ORDER by A1.city, A2.city
