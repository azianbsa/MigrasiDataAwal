SELECT
 @idpdam,
 b.id AS idblok,
 b.kodeblok,
 b.namablok,
 r.id AS idrayon,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 blok b
 JOIN rayon r ON r.koderayon = b.koderayon;