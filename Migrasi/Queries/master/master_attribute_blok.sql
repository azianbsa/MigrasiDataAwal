DROP TEMPORARY TABLE IF EXISTS __tmp_rayon;
CREATE TEMPORARY TABLE __tmp_rayon AS 
SELECT
@id:=@id+1 AS id,
a.koderayon
FROM
rayon a
,(SELECT @id:=0) AS id;

SELECT
@idpdam AS idpdam,
b.id AS idblok,
b.kodeblok AS kodeblok,
b.namablok AS namablok,
r.id AS idrayon,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
blok b
JOIN __tmp_rayon r ON r.koderayon = b.koderayon;