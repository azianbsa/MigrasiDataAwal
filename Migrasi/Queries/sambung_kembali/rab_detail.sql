DROP TEMPORARY TABLE IF EXISTS __tmp_sambung_kembali;
CREATE TEMPORARY TABLE __tmp_sambung_kembali AS
SELECT
@id := @id+1 AS ID,
p.nomor
FROM permohonan_sambung_kembali P
,(SELECT @id := @lastid) AS id
WHERE p.flaghapus=0;

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
IF(d.`paket`='',0,d.`paket`) AS `flagpaket`,
0 AS `flagdistribusi`,
0 AS `untuksppbdarispk`,
r.`tglrab` AS `waktuupdate`
FROM __tmp_sambung_kembali p
JOIN `rab_sambung_kembali` r ON r.nomorpermohonan=p.nomor
JOIN `detailrab` d ON d.`norab`=r.`norab`
,(SELECT @id:=@lastidrabdetail) AS id
WHERE r.`flaghapus`=0 AND r.`tglrab` IS NOT NULL