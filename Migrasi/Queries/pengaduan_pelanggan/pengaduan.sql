SET @tgladu_awal='2014-01-04';
SET @tgladu_akhir='2025-07-02';

SELECT
@idpdam AS idpdam,
a.`idpermohonan` AS idpermohonan,
t.`idtipepermohonan` AS idtipepermohonan,
COALESCE(s.`idsumberpengaduan`,1) AS idsumberpengaduan,
CONCAT(noadu,DATE_FORMAT(tgladu,'%Y%m%d'),REPLACE(jamadu,':','')) AS nomorpermohonan,
COALESCE(a.`tgladu`,' ',a.`jamadu`) AS waktupermohonan,
ray.`idrayon` AS idrayon,
kel.`idkelurahan` AS idkelurahan,
gol.`idgolongan` AS idgolongan,
dia.`iddiameter` AS iddiameter,
p.`idpelangganair` AS idpelangganair,
a.`uraian` AS keterangan,
COALESCE(u.`iduser`,-1) AS iduser,
NULL AS idnonair,
SUBSTRING_INDEX(a.`k_lat_long`,';',1) AS latitude,
SUBSTRING_INDEX(a.`k_lat_long`,';',-1) AS longitude,
NULL AS alamatmap,
0 AS flagverifikasi,
NULL AS waktuverifikasi,
0 AS flagusulan,
IF(a.`tglenttl` IS NOT NULL,'Selesai',NULL) AS statuspermohonan,
0 AS flaghapus,
a.`tgladu` AS waktuupdate
FROM `pengaduanplg` a
JOIN `maros_awal`.`pelangganmaros` p ON p.`nosamb`=a.`nosamb`
JOIN `maros_awal`.`tipepermohonan` t ON t.`kodejenisnonair`=a.`jns`
LEFT JOIN `maros_awal`.`sumberpengaduanmaros` s ON s.`namasumberpengaduan`=a.via
LEFT JOIN `kabmaros_loket4`.`pelanggan` pe ON pe.`nosamb`=p.`nosamb`
LEFT JOIN maros_awal.golonganmaros gol ON gol.kodegolongan = pe.`kodegol`
LEFT JOIN maros_awal.diametermaros dia ON dia.kodediameter = pe.kodediameter
LEFT JOIN maros_awal.rayonmaros ray ON ray.koderayon = pe.koderayon
LEFT JOIN maros_awal.kelurahanmaros kel ON kel.kodekelurahan = pe.kodekelurahan
LEFT JOIN `maros_awal`.`usermaros` u ON u.`nama`=a.`nmuseradu`
-- WHERE a.`tgladu`>=@tgladu_awal
-- AND a.`tgladu`<@tgladu_akhir