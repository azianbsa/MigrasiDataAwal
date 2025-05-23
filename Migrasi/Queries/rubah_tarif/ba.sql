SELECT
@idpdam AS idpdam,
pp.`idpermohonan` AS idpermohonan,
p.noba_pengecekan AS nomorba,
p.tglba_pengecekan AS tanggalba,
u.iduser AS iduser,
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
NULL AS `fotosignature`,
NULL AS kategoriputus,
0 AS flagbatal,
NULL AS idalasanbatal,
0 AS flag_dari_verifikasi,
'Berhasil Dikerjakan' AS statusberitaacara,
p.`tglba_pengecekan` AS waktuupdate
FROM `permohonan_rubah_gol` p
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` pp ON pp.`nomorpermohonan`=p.`nomor`
LEFT JOIN `kotaparepare_dataawal`.`master_user` u ON u.nama = SUBSTRING_INDEX(p.urutannonair,'.RUBAH_GOL.',1)
WHERE p.flag_ba_pengecekan=1