DROP TEMPORARY TABLE IF EXISTS __tmp_wilayah;
CREATE TEMPORARY TABLE __tmp_wilayah AS
SELECT
@id:=@id+1 AS id,
kodewil
FROM
wilayah
,(SELECT @id:=0) AS id;

SELECT
@idpdam,
@id:=@id+1 AS idarea,
a.kodearea,
a.area AS namaarea,
w.id AS idwilayah,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
`area` a
JOIN __tmp_wilayah w ON w.kodewil = a.kodewil
,(SELECT @id:=0) AS id;