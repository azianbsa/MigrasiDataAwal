-- permohonan_non_pelanggan_rab_detail
-- new(0, "id")
-- new(1, "idpdam")
-- new(2, "idpermohonan")
-- new(3, "tipe")
-- new(4, "kode")
-- new(5, "uraian")
-- new(6, "hargasatuan")
-- new(7, "satuan")
-- new(8, "qty")
-- new(9, "jumlah")
-- new(10, "ppn")
-- new(11, "keuntungan")
-- new(12, "jasadaribahan")
-- new(13, "total")
-- new(14, "kategori")
-- new(15, "kelompok")
-- new(16, "postbiaya")
-- new(17, "qtyrkp")
-- new(18, "flagbiayadibebankankepdam")
-- new(19, "flagdialihkankevendor")
-- new(20, "flagpaket")
-- new(21, "flagdistribusi")
-- new(22, "untuksppbdarispk")
-- new(23, "waktuupdate")

SET @idtipepermohonan=(SELECT idtipepermohonan FROM [dataawal].`master_attribute_tipe_permohonan` WHERE idpdam=@idpdam AND `kodetipepermohonan`='SAMBUNGAN_BARU_AIR');

SELECT
@id:=@id+1 AS `id`,
@idpdam AS `idpdam`,
p.idpermohonan AS `idpermohonan`,
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
FROM rab r
JOIN [dataawal].`tampung_permohonan_non_pelanggan` p ON p.`nomorpermohonan`=r.`nomorreg` AND p.idtipepermohonan=@idtipepermohonan AND p.idpdam=@idpdam
JOIN `detailrab` d ON d.`norab`=r.`norab`
,(SELECT @id:=@lastid) AS id
WHERE r.`flaghapus`=0