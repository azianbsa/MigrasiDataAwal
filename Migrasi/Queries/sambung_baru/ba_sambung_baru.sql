SELECT
@idpdam AS idpdam,
p.`idpermohonan` AS idpermohonan,
ba.`nomorba` AS nomorba,
COALESCE(ba.`tanggalba`,ba.`tglpasang`) AS tanggalba,
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
0 AS flagbatal,
NULL AS idalasanbatal,
1 AS flag_dari_verifikasi,
ba.`keteranganmeter` AS statusberitaacara,
ba.`tanggalba` AS waktuupdate
FROM `rab` ba
JOIN __tmp_sambung_baru p ON p.nomorreg=ba.nomorreg
WHERE ba.flaghapus = 0 AND ba.`nomorba` IS NOT NULL and ba.`tanggalba` is not null