SELECT
@idpdam AS idpdam,
@id:=@id+1 AS iddiameter,
administrasi AS administrasi,
pemeliharaan AS pemeliharaan,
0 AS pelayanan,
retribusi AS retribusi,
0 AS dendapakai0,
0 AS airlimbah,
NOW() AS waktuupdate
FROM
diameter,
(SELECT @id:=0) AS id;