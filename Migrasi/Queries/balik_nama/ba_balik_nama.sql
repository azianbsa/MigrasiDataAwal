SELECT
@idpdam AS idpdam,
bn.id AS idpermohonan,
ba.nomorba AS nomorba,
ba.tanggalba AS tanggalba,
bn.iduser AS iduser,
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
NULL AS statusberitaacara,
NOW() AS waktuupdate
FROM ba_balik_nama ba
JOIN __tmp_permohonan_balik_nama bn ON bn.nomor = ba.nomorpermohonan
WHERE ba.flaghapus = 0