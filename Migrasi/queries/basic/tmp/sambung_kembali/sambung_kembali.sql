SET @tgl_reg_awal='2014-11-21';
SET @tgl_reg_akhir='2025-06-20';

SELECT
@idpdam AS idpdam,
a.`idpermohonan` AS idpermohonan,
a.`idtipepermohonan` AS idtipepermohonan,
1 AS idsumberpengaduan,
a.`no_reg` AS nomorpermohonan,
a.`tgl_reg` AS waktupermohonan,
p.`idrayon` AS idrayon,
p.`idkelurahan` AS idkelurahan,
p.`idgolongan` AS idgolongan,
p.`iddiameter` AS iddiameter,
p.`idpelangganair` AS idpelangganair,
'' AS keterangan,
-1 AS iduser,
n.`idnonair` AS idnonair,
NULL AS latitude,
NULL AS longitude,
NULL AS alamatmap,
IF(a.`no_aktf`<>'-',1,0) AS flagverifikasi,
IF(a.`no_aktf`<>'-',a.`tgl_aktf`,NULL) AS waktuverifikasi,
0 AS flagusulan,
IF(a.`no_aktf`<>'-','Selesai',NULL) AS statuspermohonan,
0 AS flaghapus,
a.`tgl_reg` AS waktuupdate
FROM `sambungkembali` a
JOIN `maros_awal`.`pelangganmaros` p ON p.`nosamb`=a.nosamb
LEFT JOIN `nonairmaros` n ON n.`nomornonair`=a.`no_reg` AND n.`idjenisnonair`=9 AND n.`keterangan`='registrasi'
WHERE a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir