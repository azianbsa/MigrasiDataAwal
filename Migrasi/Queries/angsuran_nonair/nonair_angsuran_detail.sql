SET @tgl_ent_awal='2013-09-14';
SET @tgl_ent_akhir='2018-11-07';

SET SESSION net_read_timeout=600;
SET SESSION net_write_timeout=600;
SET SESSION wait_timeout=600;
SET SESSION interactive_timeout=600;

SELECT
a.`id` AS `id`,
@idpdam AS `idpdam`,
n.`id` AS `idangsuran`,
NULL AS `idnonair`,
c.`idpelangganair` AS `idpelangganair`,
NULL AS `idpelangganlimbah`,
NULL AS `idpelangganlltt`,
a.`periode` AS `kodeperiode`,
a.`termin` AS `termin`,
IF(a.`no_byr`='-',0,1) AS `statustransaksi`,
IF(a.`no_byr`='-',NULL,a.`no_byr`) AS `nomortransaksi`,
a.`tgl_byr` AS `waktutransaksi`,
YEAR(a.`tgl_byr`) AS `tahuntransaksi`,
IF(a.`no_byr`='-',NULL,-1) AS `iduser`,
IF(a.`no_byr`='-',NULL,1) AS `idloket`,
NULL AS `idkolektiftransaksi`,
NULL AS `idalasanbatal`,
a.`uraian` AS `keterangan`,
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
LEFT JOIN `kabmaros_loket4`.`pelanggan` d ON d.`nosamb`=a.`nosamb`
LEFT JOIN `maros_awal`.`rayonmaros` r ON r.`koderayon`=d.`koderayon`
LEFT JOIN maros_awal.`kelurahanmaros` k ON k.`kodekelurahan`=d.`kodekelurahan`
LEFT JOIN maros_awal.`golonganmaros` g ON g.`kodegolongan`=d.`kodegol`
LEFT JOIN maros_awal.`diametermaros` di ON di.`kodediameter`=d.`kodediameter`
WHERE n.`tgl_ent`>=@tgl_ent_awal
AND n.`tgl_ent`<@tgl_ent_akhir