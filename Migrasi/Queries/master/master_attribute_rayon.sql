DROP TEMPORARY TABLE IF EXISTS __tmp_area;
CREATE TEMPORARY TABLE __tmp_area AS 
SELECT
@id:=@id+1 AS id,
a.kodearea
FROM
`area` a
,(SELECT @id:=0) AS id;

SELECT
@idpdam,
@id:=@id+1 AS idrayon,
r.koderayon,
r.namarayon,
a.id AS idarea,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
rayon r
JOIN __tmp_area a ON a.kodearea = r.kodearea
,(SELECT @id:=0) AS id;