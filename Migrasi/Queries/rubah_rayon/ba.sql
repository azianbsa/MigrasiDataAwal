﻿-- permohonan_pelanggan_air_ba
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

SELECT
@idpdam AS idpdam,
pp.`idpermohonan` AS idpermohonan,
p.nomor_ba AS nomorba,
p.tanggal_ba AS tanggalba,
u.iduser AS iduser,
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
p.`tanggal_ba` AS waktuupdate
FROM `permohonan_rubah_rayon` p
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` pp ON pp.`nomorpermohonan`=p.nomor
LEFT JOIN `kotaparepare_dataawal`.`master_user` u ON u.nama=p.user_ba
WHERE p.`flag_ba`=1