-- permohonan_pelanggan_air_rab_detail
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

SELECT
@id:=@id+1 AS `id`,
@idpdam AS `idpdam`,
pp.`idpermohonan` AS `idpermohonan`,
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
p.`tglrab` AS `waktuupdate`
FROM `rab_sambung_kembali` p
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` pp ON pp.nomorpermohonan=p.`nomorpermohonan`
JOIN `detailrab` d ON d.`norab`=p.`norab`
,(SELECT @id:=@lastid) AS id
WHERE p.`flaghapus`=0