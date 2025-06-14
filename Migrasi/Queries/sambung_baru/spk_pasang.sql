﻿-- permohonan_non_pelanggan_spk_pasang
-- new(0, "idpdam")
-- new(1, "idpermohonan")
-- new(2, "nomorspk")
-- new(3, "tanggalspk")
-- new(4, "nomorsppb")
-- new(5, "tanggalsppb")
-- new(6, "iduser")
-- new(7, "flagbatal")
-- new(8, "idalasanbatal")
-- new(9, "waktuupdate")

SET @idtipepermohonan=(SELECT idtipepermohonan FROM `kotaparepare_dataawal`.`master_attribute_tipe_permohonan` WHERE idpdam=@idpdam AND `kodetipepermohonan`='SAMBUNGAN_BARU_AIR');

SELECT
@idpdam AS `idpdam`,
p.idpermohonan AS `idpermohonan`,
r.`nomorspkp` AS `nomorspk`,
r.`tanggalspkp` AS `tanggalspk`,
r.`nomorsppb` AS `nomorsppb`,
r.`tanggalspkp` AS `tanggalsppb`,
u.iduser AS `iduser`, -- harus ambil dari logakses bukan rab, karna ini user pembuat rab bukan user pembuat spkp
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
r.tanggalspkp AS `waktuupdate`
FROM rab r
JOIN `kotaparepare_dataawal`.`tampung_permohonan_non_pelanggan` p ON p.`nomorpermohonan`=r.`nomorreg` AND p.idtipepermohonan=@idtipepermohonan AND p.idpdam=@idpdam
LEFT JOIN `kotaparepare_dataawal`.`master_user` u ON u.nama=r.`user`
WHERE r.flaghapus=0 AND r.nomorspkp IS NOT NULL