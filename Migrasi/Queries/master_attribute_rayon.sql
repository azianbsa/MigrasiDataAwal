SELECT
 @idpdam,
 r.id AS idrayon,
 r.koderayon,
 r.namarayon,
 a.id AS idarea,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 rayon r
 JOIN `area` a ON a.kodearea = r.area;