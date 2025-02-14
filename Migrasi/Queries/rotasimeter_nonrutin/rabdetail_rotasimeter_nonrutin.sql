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
l.`tglrab` AS `waktuupdate`
FROM
`rotasimeter_nonrutin` r
JOIN rab_lainnya l ON l.`norab`=r.`norab`
JOIN `detailrab` d ON d.`norab`=r.`norab`
JOIN __tmp_rotasimeter p ON p.`nosamb`=r.`nosamb` AND p.`periode`=r.`periode`,
(SELECT @id:=@lastid) AS id
WHERE l.`tglrab` IS NOT NULL