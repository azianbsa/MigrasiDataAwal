SELECT
@idpdam AS idpdam,
per.`idpermohonan` AS idpermohonan,
ba.`nomorba` AS nomorba,
ba.`tanggalba` AS tanggalba,
NULL AS iduser,
NULL AS persilnamapaket,
0 AS persilflagdialihkankevendor,
0 AS persilflagbiayadibebankankepdam,
NULL AS distribusinamapaket,
0 AS distribusiflagdialihkankevendor,
0 AS distribusiflagbiayadibebankankepdam,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
NULL AS kategoriputus,
0 AS flagbatal,
NULL AS idalasanbatal,
1 AS flag_dari_verifikasi,
ba.`memo` AS statusberitaacara,
ba.`tanggalba` AS waktuupdate
FROM `ba_sambungkembali` ba
JOIN __tmp_sambung_kembali per ON per.nomor=ba.nomorpermohonan
WHERE ba.flaghapus = 0