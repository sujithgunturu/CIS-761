DROP TABLE IF EXISTS rental;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS plan;


CREATE TABLE plan(
  pid integer PRIMARY KEY,
  name VARCHAR(50) UNIQUE NOT NULL,
  max_movies int NOT NULL,
  fee numeric(6,2) NOT NULL
);

CREATE TABLE customer(
  cid integer PRIMARY KEY,
  login VARCHAR(50),
  password VARCHAR(50),
  fname VARCHAR(50),
  lname VARCHAR(50),
  pid integer REFERENCES plan (pid)
);

-- in mysql
-- CREATE TABLE rental(
--   mid VARCHAR(50) NOT NULL,
--   cid integer REFERENCES customer(cid),
--   date_out DATETIME NOT NULL, 
--   status VARCHAR(50) CHECK (status = 'open' or status = 'closed')
-- );

--different in postgresql 
CREATE TABLE rental(
  mid VARCHAR(50) NOT NULL,
  cid integer REFERENCES customer(cid),
  date_out TIMESTAMP NOT NULL, 
  status VARCHAR(50) CHECK (status = 'open' or status = 'closed')
);



INSERT INTO plan VALUES (1, 'basic', 1, 100);
INSERT INTO plan VALUES (2, 'rental plus', 3, 200);
INSERT INTO plan VALUES (3, 'super access', 5, 300);
INSERT INTO plan VALUES (4, 'kansas super user', 10, 400); 

INSERT INTO customer VALUES (1, 'sguntur', 'pass1', 'sujith', 'gunturu', 1);
INSERT INTO customer VALUES (2, 'madhu', 'pass2', 'madhu', 'latha', 1);


INSERT INTO rental VALUES('M_111', 1,  '2020-03-20 11:32:14', 'open');
INSERT INTO rental VALUES('M_122', 1,  '2020-04-02 19:17:55', 'closed');
