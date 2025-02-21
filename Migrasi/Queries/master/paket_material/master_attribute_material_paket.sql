SELECT
@idpdam,
@id := @id+1 AS idpaketmaterial,
@id AS kodepaketmaterial,
namapaket AS namapaketmaterial,
NULL AS keterangan,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
paketmaterial
,(SELECT @id := 0) AS id
GROUP BY namapaket;