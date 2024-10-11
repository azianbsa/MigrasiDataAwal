SELECT
 @idpdam,
 id AS iddiameter,
 kodediameter,
 ukuran AS namadiameter,
 periodemulaiberlaku,
 aktif AS STATUS,
 '' AS nomorsk,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 diameter
 WHERE aktif=1