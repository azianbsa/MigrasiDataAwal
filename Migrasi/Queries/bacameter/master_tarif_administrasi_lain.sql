SELECT
 @idpdam,
 id AS idadministrasilain,
 kode AS kodeadministrasilain,
 keterangan AS namaadministrasilain,
 administrasilain AS administrasi,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 byadministrasi_lain;