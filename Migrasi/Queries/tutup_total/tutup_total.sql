SET @tgl_reg_awal='2025-06-26';
SET @tgl_reg_akhir='2025-06-29';

SELECT
@idpdam AS idpdam,
g.`idpermohonan` AS idpermohonan,
56 AS idtipepermohonan,
1 AS idsumberpengaduan,
g.`nomorpermohonan` AS nomorpermohonan,
a.`tgl_cm` AS waktupermohonan,
ray.`idrayon` AS idrayon,
kel.`idkelurahan` AS idkelurahan,
gol.`idgolongan` AS idgolongan,
dia.`iddiameter` AS iddiameter,
p.`idpelangganair` AS idpelangganair,
'' AS keterangan,
-1 AS iduser,
NULL AS idnonair,
NULL AS latitude,
NULL AS longitude,
NULL AS alamatmap,
IF(a.`tgl_cm` IS NOT NULL,1,0) AS flagverifikasi,
a.`tgl_cm` AS waktuverifikasi,
0 AS flagusulan,
IF(a.`tgl_cm` IS NOT NULL,'Selesai',NULL) AS statuspermohonan,
0 AS flaghapus,
a.`tgl_cm` AS waktuupdate
FROM `t_reg_cm` a
JOIN `maros_awal`.`pelangganmaros` p ON p.`nosamb`=a.`nosamb`
JOIN `tutuptotaltunggakan` g ON g.`no_cm`=a.`no_cm`
LEFT JOIN `kabmaros_loket4`.`pelanggan` pe ON pe.`nosamb`=p.`nosamb`
LEFT JOIN maros_awal.golonganmaros gol ON gol.kodegolongan = pe.`kodegol`
LEFT JOIN maros_awal.diametermaros dia ON dia.kodediameter = pe.kodediameter
LEFT JOIN maros_awal.rayonmaros ray ON ray.koderayon = pe.koderayon
LEFT JOIN maros_awal.kelurahanmaros kel ON kel.kodekelurahan = pe.kodekelurahan
WHERE a.`tgl_cm`>=@tgl_reg_awal
AND a.`tgl_cm`<@tgl_reg_akhir