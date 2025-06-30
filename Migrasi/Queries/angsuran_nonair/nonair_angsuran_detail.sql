SET @tgl_ent_awal='2014-01-01';
SET @tgl_ent_akhir='2018-11-07';

SET SESSION net_read_timeout=600;
SET SESSION net_write_timeout=600;
SET SESSION wait_timeout=600;
SET SESSION interactive_timeout=600;

SELECT
a.`id` AS `id`,
@idpdam AS `idpdam`,
n.`idangs` AS `idangsuran`,
NULL AS `idnonair`,
c.`idpelangganair` AS `idpelangganair`,
NULL AS `idpelangganlimbah`,
NULL AS `idpelangganlltt`,
IF(a.`lunas`,DATE_FORMAT(a.`tgl_byr`,'%Y%m'),NULL) `kodeperiode`,
a.`angs_ke` AS `termin`,
a.`lunas` AS `statustransaksi`,
a.`no_byr` AS `nomortransaksi`,
a.`tgl_byr` AS `waktutransaksi`,
YEAR(a.`tgl_byr`) AS `tahuntransaksi`,
-1 AS `iduser`,
1 AS `idloket`,
NULL AS `idkolektiftransaksi`,
NULL AS `idalasanbatal`,
CONCAT(a.`angs_ke`,'/',a.`angs_tot`) AS `keterangan`,
r.`idrayon` AS `idrayon`,
k.`idkelurahan` AS `idkelurahan`,
g.`idgolongan` AS `idgolongan`,
di.`iddiameter` AS `iddiameter`,
a.`tgl_ent` AS `tglmulaitagih`,
0 AS `biayabahan`,
0 AS `biayapasang`,
0 AS `ujl`,
0 AS `ppn`,
0 AS `pendaftaran`,
0 AS `administrasi`,
0 AS `meterai`,
0 AS `lainnya`,
a.`jml_dibayar` AS `total`,
a.`tgl_ent` AS `waktuupdate`
FROM `angsurannonairdetail` a
JOIN `angsurannonair` n ON n.`nosamb`=a.`nosamb`
LEFT JOIN `maros_awal`.`pelangganmaros` c ON c.`nosamb`=a.`nosamb`
LEFT JOIN `kabmaros_bsbs4`.`pelanggan` d ON d.`nosamb`=a.`nosamb`
LEFT JOIN `maros_awal`.`rayonmaros` r ON r.`koderayon`=d.`koderayon`
LEFT JOIN maros_awal.`kelurahanmaros` k ON k.`kodekelurahan`=d.`kodekelurahan`
LEFT JOIN maros_awal.`golonganmaros` g ON g.`kodegolongan`=d.`kodegol`
LEFT JOIN maros_awal.`diametermaros` di ON di.`kodediameter`=d.`kodediameter`
WHERE n.`tgl_ent`>=@tgl_ent_awal
AND n.`tgl_ent`<@tgl_ent_akhir