select name from Highschooler
where grade = '10'
ORDER BY name;



SELECT AVG(count)
FROM (
  SELECT COUNT(*) AS count
  FROM Likes
  GROUP BY ID2
) AS T;



SELECT name, grade
FROM Highschooler
INNER JOIN Likes ON Highschooler.ID = Likes.ID2
GROUP BY ID2
HAVING COUNT(*) = (
  SELECT MAX(count)
  FROM (
    SELECT COUNT(*) AS count
    FROM Likes
    GROUP BY ID2
  ) AS T1 );

SELECT name, grade
FROM Highschooler H1
WHERE ID NOT IN (
  SELECT ID1
  FROM Friend, Highschooler H2
  WHERE H1.ID = Friend.ID1 AND H2.ID = Friend.ID2 AND H1.grade <> H2.grade
);
 SELECT name, grade
FROM Highschooler H1
WHERE grade NOT IN (
  SELECT H2.grade
  FROM Friend, Highschooler H2
  WHERE H1.ID = Friend.ID1 AND H2.ID = Friend.ID2
);

select name, grade 
from (select ID2, count(ID2) as numLiked from Likes group by ID2) AS T, Highschooler
where numLiked>1 and ID2=ID
ORDER BY grade, name;


SELECT H1.name, COUNT(H2.ID) AS C
FROM Highschooler H1
INNER JOIN Friend ON H1.ID = Friend.ID1	
INNER JOIN Highschooler H2 ON H2.ID = Friend.ID2
GROUP BY (H1.ID)
HAVING C>1;



SET @cassandra=(select id from Highschooler where name='Cassandra');
SELECT name from Highschooler WHERE
ID IN (
select * from (
select f1.id1
from Friend f1
where f1.id2 = @cassandra
and f1.id1 != @cassandra
union
select f2.id2
from Friend f2
where f2.id1 in (select f1.id1 from Friend f1 where f1.id2 = @cassandra)
and f2.id2 != @cassandra
) T);




SELECT name, grade
FROM Highschooler
WHERE ID NOT IN (
  SELECT DISTINCT ID1
  FROM Likes
  UNION
  SELECT DISTINCT ID2
  FROM Likes
)
ORDER BY grade, name;


SELECT H1.name, H1.grade, H2.name, H2.grade
FROM Highschooler H1
INNER JOIN Likes ON H1.ID = Likes.ID1
INNER JOIN Highschooler H2 ON H2.ID = Likes.ID2
WHERE (H1.ID = Likes.ID1 AND H2.ID = Likes.ID2) AND H2.ID NOT IN (
  SELECT DISTINCT ID1
  FROM Likes
);



