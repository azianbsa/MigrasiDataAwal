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
NULL AS flag_dari_verifikasi,
'Berhasil Dikerjakan' AS statusberitaacara,
r.tglpasang AS waktuupdate
FROM `rab_sambung_kembali` r
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` p ON p.nomorpermohonan=r.`nomorpermohonan`
LEFT JOIN `kotaparepare_dataawal`.`master_user` u ON u.nama=r.user AND u.idpdam=@idpdam
WHERE r.flaghapus=0 AND r.flagpasang=1