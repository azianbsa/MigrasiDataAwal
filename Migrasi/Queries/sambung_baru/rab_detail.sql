DROP TABLE IF EXISTS __tmp_pendaftaran;
CREATE TABLE __tmp_pendaftaran AS
SELECT
@id:=@id+1 AS id,
nomorreg
FROM `pendaftaran`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

SELECT
@id:=@id+1 AS `id`,
@idpdam AS `idpdam`,
p.id AS `idpermohonan`,
d.`tipe` AS `tipe`,
d.`kode` AS `kode`,
d.`nama` AS `uraian`,
d.harga AS `hargasatuan`,
d.`satuan` AS `satuan`,
d.qty AS `qty`,
d.`jumlah` AS `jumlah`,
d.`ppn` AS `ppn`,
d.`keuntungan` AS `keuntungan`,
0 AS `jasadaribahan`,
d.`jumlah` AS `total`,
'-' AS `kategori`,
'-' AS `kelompok`,
d.`kategori` AS `postbiaya`,
d.`qty_rkp` AS `qtyrkp`,
IF(d.`dibebankan_pdam`=1,1,0) AS `flagbiayadibebankankepdam`,
IF(d.`vendor`=1,1,0) AS `flagdialihkankevendor`,
d.`paket` AS `flagpaket`,
0 AS `flagdistribusi`,
0 AS `untuksppbdarispk`,
r.`tglrab` AS `waktuupdate`
FROM __tmp_pendaftaran p
JOIN `rab` r ON r.`nomorreg`=p.`nomorreg`
JOIN `detailrab` d ON d.`norab`=r.`norab`
,(SELECT @id:=@lastid) AS id
WHERE r.`flaghapus`=0 AND r.`tglrab` IS NOT NULL