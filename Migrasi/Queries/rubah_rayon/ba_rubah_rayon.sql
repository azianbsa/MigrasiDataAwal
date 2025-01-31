DROP TEMPORARY TABLE IF EXISTS __tmp_userloket;
CREATE TEMPORARY TABLE __tmp_userloket (
    iduser INT,
    nama VARCHAR(30),
    INDEX idx_tmp_userloket_nama (nama)
);
INSERT INTO __tmp_userloket
SELECT
@iduser := @iduser + 1 AS iduser,
nama
FROM userloket
,(SELECT @iduser := 0) AS iduser
ORDER BY nama;

SELECT
@idpdam AS idpdam,
@id := @id+1 AS idpermohonan,
per.nomor_ba AS nomorba,
per.tanggal_ba AS tanggalba,
usr.iduser AS iduser,
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
FROM permohonan_rubah_rayon per
LEFT JOIN __tmp_userloket usr ON usr.nama = per.user_ba
,(SELECT @id := @lastid) AS id
WHERE per.flaghapus = 0