SELECT
@idpdam AS idpdam,
pp.`idpermohonan` AS idpermohonan,
p.`nomorba` AS nomorba,
p.`tanggalba` AS tanggalba,
u.`iduser` AS iduser,
NULL AS persilnamapaket,
0 AS persilflagdialihkankevendor,
0 AS persilflagbiayadibebankankepdam,
NULL AS distribusinamapaket,
0 AS distribusiflagdialihkankevendor,
0 AS distribusiflagbiayadibebankankepdam,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
NULL AS `fotobukti4`,
NULL AS `fotobukti5`,
NULL AS `fotobukti6`,
NULL AS fotosignature,
NULL AS kategoriputus,
0 AS flagbatal,
NULL AS idalasanbatal,
0 AS flag_dari_verifikasi,
'Berhasil Dikerjakan' AS statusberitaacara,
p.`tanggalba` AS waktuupdate
FROM ba_pemutusan_sementara p
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` pp ON pp.nomorpermohonan=p.`nomorpermohonan`
LEFT JOIN `kotaparepare_dataawal`.`master_user` u ON u.`nama`=p.`user`
WHERE p.`flaghapus`=0