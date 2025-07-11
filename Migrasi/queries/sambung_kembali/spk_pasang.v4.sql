-- permohonan_pelanggan_air_spk_pasang
-- new(0, "idpdam")
-- new(1, "idpermohonan")
-- new(2, "nomorspk")
-- new(3, "tanggalspk")
-- new(4, "nomorsppb")
-- new(5, "tanggalsppb")
-- new(6, "iduser,")
-- new(7, "flagbatal")
-- new(8, "idalasanbatal")
-- new(9, "waktuupdate")

SELECT
@idpdam AS `idpdam`,
p.idpermohonan AS `idpermohonan`,
r.`nomorspkp` AS `nomorspk`,
r.`tanggalspk` AS `tanggalspk`,
r.`nomorsppb` AS `nomorsppb`,
r.`tanggalspk` AS `tanggalsppb`,
u.iduser AS `iduser`, -- harus ambil dari logakses bukan rab, karna ini user pembuat rab bukan user pembuat spkp
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
r.tanggalspk AS `waktuupdate`
FROM `rab_sambung_kembali` r
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` p ON p.`nomorpermohonan`=r.`nomorpermohonan`
LEFT JOIN `kotaparepare_dataawal`.`master_user` u ON u.nama=r.`user`
WHERE r.flaghapus=0 AND r.`flagaktif`=1