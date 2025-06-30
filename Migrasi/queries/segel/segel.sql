SET @tgl_reg_awal='2002-01-01';
SET @tgl_reg_akhir='2019-01-01';

SELECT
@idpdam AS idpdam,
g.`idpermohonan` AS idpermohonan,
72 AS idtipepermohonan,
1 AS idsumberpengaduan,
g.`nomorpermohonan` AS nomorpermohonan,
a.`tgl_sm` AS waktupermohonan,
ray.`idrayon` AS idrayon,
kel.`idkelurahan` AS idkelurahan,
gol.`idgolongan` AS idgolongan,
dia.`iddiameter` AS iddiameter,
p.`idpelangganair` AS idpelangganair,
a.ket AS keterangan,
-1 AS iduser,
NULL AS idnonair,
NULL AS latitude,
NULL AS longitude,
NULL AS alamatmap,
IF(a.`tgl_sm` IS NOT NULL,1,0) AS flagverifikasi,
a.`tgl_sm` AS waktuverifikasi,
0 AS flagusulan,
IF(a.`tgl_sm` IS NOT NULL,'Selesai',NULL) AS statuspermohonan,
0 AS flaghapus,
a.`tgl_sm` AS waktuupdate
FROM `t_reg_sm` a
JOIN `maros_awal`.`pelangganmaros` p ON p.`nosamb`=a.`nosamb`
JOIN `segeltunggakan` g ON g.`no_sm`=a.`no_sm`
LEFT JOIN `kabmaros_loket4`.`pelanggan` pe ON pe.`nosamb`=p.`nosamb`
LEFT JOIN maros_awal.golonganmaros gol ON gol.kodegolongan = pe.`kodegol`
LEFT JOIN maros_awal.diametermaros dia ON dia.kodediameter = pe.kodediameter
LEFT JOIN maros_awal.rayonmaros ray ON ray.koderayon = pe.koderayon
LEFT JOIN maros_awal.kelurahanmaros kel ON kel.kodekelurahan = pe.kodekelurahan
WHERE a.`tgl_sm`>=@tgl_reg_awal
AND a.`tgl_sm`<@tgl_reg_akhir