SELECT
 @idpdam,
 @id := @id+1 AS idpaketongkos,
 @id AS kodepaketongkos,
 namapaket AS namapaketongkos,
 NULL AS keterangan,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 paketongkos
 ,(SELECT @id := 0) AS id
 GROUP BY namapaket