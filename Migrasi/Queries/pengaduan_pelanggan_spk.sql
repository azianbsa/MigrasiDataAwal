SELECT
 @idpdam,
 pgd.id AS idpermohonan,
 spk.nomorspk,
 spk.tanggalspk,
 -1 AS iduser,
 0 AS flagsurvey,
 NULL AS fotobukti1,
 NULL AS fotobukti2,
 NULL AS fotobukti3,
 0 AS flagbatal,
 NULL AS idalasanbatal,
 NOW() AS waktuupdate
FROM
 spk_pengaduan spk
 LEFT JOIN pengaduan pgd ON pgd.nomor = spk.nomorpengaduan
 WHERE spk.flaghapus = 0