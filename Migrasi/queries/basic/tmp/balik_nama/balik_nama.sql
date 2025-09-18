SET @tgl_reg_awal='2017-01-01';
SET @tgl_reg_akhir='2025-07-01';

SELECT
@idpdam AS idpdam,
b.`idpermohonan` AS idpermohonan,
2 AS idtipepermohonan,
1 AS idsumberpengaduan,
a.no_reg AS nomorpermohonan,
a.tgl_reg AS waktupermohonan,
NULL AS idrayon,
NULL AS idkelurahan,
NULL AS idgolongan,
NULL AS iddiameter,
p.`idpelangganair` AS idpelangganair,
a.alasan AS keterangan,
-1 AS iduser,
n.`idnonair` AS  idnonair,
NULL AS latitude,
NULL AS longitude,
NULL AS alamatmap,
IF(a.`tgl_bn` IS NOT NULL,1,0) AS flagverifikasi,
a.`tgl_bn` AS waktuverifikasi,
0 AS flagusulan,
IF(a.`tgl_bn` IS NOT NULL,'Selesai',NULL) AS statuspermohonan,
0 AS flaghapus,
a.tgl_reg AS waktuupdate
FROM `t_reg_bn` a
JOIN `baliknama` b ON b.`no_reg`=a.`no_reg`
LEFT JOIN `pelangganmaros` p ON p.`nosamb`=a.nosamb
LEFT JOIN `migrasi_nonair` n ON n.`no_reg`=a.`no_reg`
WHERE a.tgl_reg>=@tgl_reg_awal
AND a.tgl_reg<@tgl_reg_akhir