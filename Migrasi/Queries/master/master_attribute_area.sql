SELECT
 @idpdam,
 a.id AS idarea,
 a.kodearea,
 a.area AS namaarea,
 w.id AS idwilayah,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 `area` a
 JOIN wilayah w ON w.kodewil = a.kodewil;