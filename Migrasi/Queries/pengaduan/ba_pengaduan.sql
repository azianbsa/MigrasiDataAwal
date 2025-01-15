SELECT
@idpdam,
pgd.id AS idpermohonan,
ba.nomorba,
ba.tgldiselesaikan AS tanggalba,
-1 AS iduser,
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
NULL AS flag_dari_verifikasi,
CASE
WHEN STATUS='Dapat Di Kerjakan' THEN 'Berhasil Dikerjakan' 
WHEN STATUS='Tidak Dapat Dikerjakan' THEN 'Tidak Berhasil Dikerjakan'
END AS statusberitaacara,
NOW() AS waktuupdate
FROM
spk_pengaduan ba
JOIN pengaduan pgd ON pgd.nomor = ba.nomorpengaduan
WHERE ba.flaghapus = 0 AND ba.nomorba IS NOT NULL