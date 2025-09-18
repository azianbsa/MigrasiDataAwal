SET @tgl_ent_awal='2013-09-14';
SET @tgl_ent_akhir='2018-11-07';

SET SESSION net_read_timeout=600;
SET SESSION net_write_timeout=600;
SET SESSION wait_timeout=600;
SET SESSION interactive_timeout=600;

SELECT
@idpdam AS `idpdam`,
n.`id` AS `idangsuran`,
n.`noreg` AS `noangsuran`,
NULL AS `idnonair`,
-1 AS `idjenisnonair`,
b.idpelangganair AS `idpelangganair`,
NULL AS `idpelangganlimbah`,
NULL AS `idpelangganlltt`,
n.`nama` AS `nama`,
n.`alamat` AS `alamat`,
'' AS `notelp`,
'' AS `nohp`,
n.`nama` AS `namapemohon`,
n.`alamat` AS `alamatpemohon`,
'' AS `notelppemohon`,
'' AS `nohppemohon`,
n.`tgl_ent` AS `waktudaftar`,
n.`angs_tot` AS `jumlahtermin`,
n.`jml_tag` AS `jumlahangsuranpokok`,
0 AS `jumlahangsuranbunga`,
n.`uangmuka` AS `jumlahuangmuka`,
n.`jml_tag` AS `total`,
-1 AS `iduser`,
n.`tgl_ent` AS `tglmulaitagihpertama`,
n.`noreg` AS `noberitaacara`,
n.`tgl_ent` AS `tglberitaacara`,
1 AS `flagpublish`,
n.`tgl_ent` AS `waktupublish`,
n.`lunas` AS `flaglunas`,
n.`tgl_lunas` AS `waktulunas`,
0 AS `flaghapus`,
n.`tgl_ent` AS `waktuupdate`
FROM `angsurannonair` n
LEFT JOIN `maros_awal`.`pelangganmaros` b ON b.nosamb=n.`nosamb`
WHERE n.`tgl_ent`>=@tgl_ent_awal
AND n.`tgl_ent`<@tgl_ent_akhir
