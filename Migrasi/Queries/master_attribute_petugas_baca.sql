SELECT
 @idpdam,
 @id:=@id+1 AS idpetugasbaca,
 @id AS kodepetugasbaca,
 nama AS petugasbaca,
 LOWER(REPLACE(nama,' ','_')) AS namauser,
 1 AS PASSWORD,
 'Pembaca Meter' AS jenispembaca,
 alamat,
 '0001-01-01' AS tgllahir,
 nohp,
 '0001-01-01' AS tglmulaikerja,
 NULL AS fotopetugasbaca,
 '' AS keterangan,
 1 AS STATUS,
 0 AS flaghapus,
 NOW() AS waktucreate,
 NOW() AS waktuupdate
FROM
 pembacameter
 ,(SELECT @id:=0) AS id;