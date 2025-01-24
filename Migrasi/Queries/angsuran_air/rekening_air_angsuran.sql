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
@idpdam,
ang.id AS idangsuran,
ang.noangsuran AS noangsuran,
@jnsnonair AS idjenisnonair,
pel.id AS idpelangganair,
ang.nama AS nama,
ang.alamat AS alamat,
ang.notelp AS notelp,
ang.nohp AS nohp,
ang.waktudaftar AS waktudaftar,
ang.jumlahtermin AS jumlahtermin,
ang.jumlahangsuranpokok AS jumlahangsuranpokok,
ang.jumlahangsuranbunga AS jumlahangsuranbunga,
ang.jumlahuangmuka AS jumlahuangmuka,
ang.jumlah as total,
usr.iduser AS iduser,
ang.tglmulaitagih AS tglmulaitagihpertama,
ba.nomorba AS noberitaacara,
ba.tanggalba AS tglberitaacara,
1 AS flagpublish,
ang.waktudaftar AS waktupublish,
ang.flaglunas AS flaglunas,
ang.waktulunas AS waktulunas,
0 AS flaghapus,
IFNULL(ang.waktulunas,ang.waktudaftar) AS waktuupdate
FROM daftarangsuran ang
JOIN [bsbs].pelanggan pel ON pel.nosamb = ang.dibebankankepada
LEFT JOIN __tmp_userloket usr ON usr.nama = ang.userdaftar
LEFT JOIN ba_angsuran ba ON ba.noangsuran=ang.noangsuran and ba.flaghapus=0
WHERE ang.keperluan='JNS-36'