DROP TEMPORARY TABLE IF EXISTS __tmp_pengaduan;
CREATE TEMPORARY TABLE __tmp_pengaduan AS
SELECT
@id:=@id+1 AS id,
p.nomor,
p.`kategori`
FROM pengaduan p
JOIN `__tmp_tipepermohonan` t ON t.`kodejenisnonair`=p.`kategori`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0 AND `flagpel`=1;

DROP TEMPORARY TABLE IF EXISTS __tmp_userloket;
CREATE TEMPORARY TABLE __tmp_userloket AS
SELECT
@idpdam,
@id := @id + 1 AS iduser,
a.nama,
a.namauser
FROM (
SELECT nama,namauser,`passworduser`,alamat,aktif FROM [bacameter].`userakses`
UNION
SELECT nama,namauser,`passworduser`,NULL AS alamat,aktif FROM [bsbs].`userakses`
UNION
SELECT nama,namauser,`passworduser`,NULL AS alamat,flagaktif AS aktif FROM `userloket`
UNION
SELECT nama,namauser,`passworduser`,NULL AS alamat,flagaktif AS aktif FROM `userbshl`
) a,
(SELECT @id := 0) AS id
GROUP BY a.namauser;

SELECT
@idpdam,
p.id AS idpermohonan,
b.nomorba,
b.tgldiselesaikan AS tanggalba,
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
NULL AS kategoriputus,
0 AS flagbatal,
NULL AS idalasanbatal,
0 AS flag_dari_verifikasi,
CASE
WHEN b.status='Dapat Di Kerjakan' THEN 'Berhasil Dikerjakan' 
WHEN b.status='Tidak Dapat Dikerjakan' THEN 'Tidak Berhasil Dikerjakan'
WHEN b.status IS NULL THEN 'Berhasil Dikerjakan' 
END AS statusberitaacara,
COALESCE(b.tgldiselesaikan,NOW()) AS waktuupdate
FROM
__tmp_pengaduan p
JOIN spk_pengaduan b ON b.nomorpengaduan=p.nomor
LEFT JOIN __tmp_userloket u ON u.nama=b.user_ba
WHERE b.flaghapus=0 AND b.nomorba IS NOT NULL AND b.tgldiselesaikan IS NOT NULL