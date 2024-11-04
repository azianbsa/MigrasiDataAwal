ALTER TABLE pelanggan
 ADD COLUMN id VARCHAR (50) NULL FIRST;
 
UPDATE pelanggan a
JOIN (
	SELECT
	@id:=@id+1 AS id,
	nosamb
	FROM pelanggan
	,(SELECT @id:=0) AS id ORDER BY nosamb
) b ON b.nosamb=a.nosamb
SET a.id=b.id;

ALTER TABLE pelanggan
 CHANGE id id VARCHAR (50) CHARSET latin1 COLLATE latin1_swedish_ci NOT NULL,
 DROP PRIMARY KEY,
 ADD PRIMARY KEY (id, nosamb);