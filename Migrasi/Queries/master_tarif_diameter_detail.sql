SELECT
 @idpdam,
 id AS iddiameter,
 administrasi,
 pemeliharaan,
 pelayanan,
 retribusi,
 dendapakai0,
 airlimbah,
 NOW() AS waktuupdate
FROM
 diameter
 WHERE aktif=1