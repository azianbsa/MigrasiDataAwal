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
per.nomor_spk AS nomorspk,
per.tanggal_spk AS tanggalspk,
NULL AS `nomorsppb`,
NULL AS `tanggalsppb`,
usr.iduser AS iduser,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
0 AS flagbatal,
NULL AS idalasanbatal,
NOW() AS waktuupdate
FROM permohonan_rubah_rayon per
LEFT JOIN __tmp_userloket usr ON usr.nama = per.user_spk
,(SELECT @id := @lastid) AS id
WHERE per.flaghapus = 0