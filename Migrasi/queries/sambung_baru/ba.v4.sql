-- permohonan_non_pelanggan_ba
-- new(0, "idpdam")
-- new(1, "idpermohonan")
-- new(2, "nomorba")
-- new(3, "tanggalba")
-- new(4, "iduser")
-- new(5, "persilnamapaket")
-- new(6, "persilflagdialihkankevendor")
-- new(7, "persilflagbiayadibebankankepdam")
-- new(8, "distribusinamapaket")
-- new(9, "distribusiflagdialihkankevendor")
-- new(10, "distribusiflagbiayadibebankankepdam")
-- new(11, "flagbatal")
-- new(12, "idalasanbatal")
-- new(13, "flag_dari_verifikasi")
-- new(14, "statusberitaacara")
-- new(15, "waktuupdate")

SET @idtipepermohonan=(SELECT idtipepermohonan FROM `kotaparepare_dataawal`.`master_attribute_tipe_permohonan` WHERE idpdam=@idpdam AND `kodetipepermohonan`='SAMBUNGAN_BARU_AIR');

SELECT
@idpdam AS idpdam,
p.idpermohonan AS idpermohonan,
r.`nomorba` AS nomorba,
r.tglpasang AS tanggalba,
u.iduser AS iduser, -- harus ambil dari logakses bukan rab, karna ini user pembuat rab bukan user pembuat ba
NULL AS persilnamapaket,
0 AS persilflagdialihkankevendor,
0 AS persilflagbiayadibebankankepdam,
NULL AS distribusinamapaket,
0 AS distribusiflagdialihkankevendor,
0 AS distribusiflagbiayadibebankankepdam,
0 AS flagbatal,
NULL AS idalasanbatal,
NULL AS flag_dari_verifikasi,
'Berhasil Dikerjakan' AS statusberitaacara,
r.tglpasang AS waktuupdate
FROM rab r
JOIN `kotaparepare_dataawal`.`tampung_permohonan_non_pelanggan` p ON p.nomorpermohonan=r.`nomorreg` AND p.idtipepermohonan=@idtipepermohonan AND p.idpdam=@idpdam
LEFT JOIN `kotaparepare_dataawal`.`master_user` u ON u.nama=r.user AND u.idpdam=@idpdam
WHERE r.flaghapus=0 AND r.flagpasang=1