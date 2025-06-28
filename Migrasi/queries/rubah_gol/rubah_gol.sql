SET @tgl_reg_awal='2017-12-18';
SET @tgl_reg_akhir='2025-01-10';

SELECT
@idpdam AS idpdam,
g.`idpermohonan` AS idpermohonan,
74 AS idtipepermohonan,
1 AS idsumberpengaduan,
a.`no_reg` AS nomorpermohonan,
a.`tgl_reg` AS waktupermohonan,
ray.`idrayon` AS idrayon,
kel.`idkelurahan` AS idkelurahan,
gol.`idgolongan` AS idgolongan,
dia.`iddiameter` AS iddiameter,
p.`idpelangganair` AS idpelangganair,
a.`alasan` AS keterangan,
-1 AS iduser,
n.`idnonair` AS idnonair,
NULL AS latitude,
NULL AS longitude,
NULL AS alamatmap,
IF(a.`tgl_pg` IS NOT NULL,1,0) AS flagverifikasi,
a.`tgl_pg` AS waktuverifikasi,
0 AS flagusulan,
IF(a.`tgl_pg` IS NOT NULL,'Selesai',NULL) AS statuspermohonan,
0 AS flaghapus,
a.`tgl_reg` AS waktuupdate
FROM `t_reg_pg` a
JOIN `maros_awal`.`pelangganmaros` p ON p.`nosamb`=a.`nosamb`
JOIN `rubahgol` g ON g.`no_reg`=a.`no_reg`
LEFT JOIN `kabmaros_loket4`.`pelanggan` pe ON pe.`nosamb`=p.`nosamb`
LEFT JOIN `t_goltarif` t ON t.`KDGT`=a.`kd_gol_lama`
LEFT JOIN maros_awal.golonganmaros gol ON gol.kodegolongan = t.`GOLTARIF`
LEFT JOIN maros_awal.diametermaros dia ON dia.kodediameter = pe.kodediameter
LEFT JOIN maros_awal.rayonmaros ray ON ray.koderayon = pe.koderayon
LEFT JOIN maros_awal.kelurahanmaros kel ON kel.kodekelurahan = pe.kodekelurahan
LEFT JOIN `migrasi_nonair` n ON n.`no_reg`=a.`no_reg`
WHERE a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir