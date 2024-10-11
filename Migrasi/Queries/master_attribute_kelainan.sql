SELECT
 @idpdam,
 id AS idkelainan,
 id AS kodekelainan,
 kelainan,
 'Biasa' AS jeniskelainan,
 '' AS Deskripsi,
 0 AS posisi,
 0 AS blokir,
 1 AS STATUS,
 0 AS taksirotomatis,
 0 AS requestbacaulangotomatis,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 kelainan;