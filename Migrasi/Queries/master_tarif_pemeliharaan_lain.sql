SELECT
 @idpdam,
 id AS idpemeliharaanlain,
 kode AS kodepemeliharaanlain,
 keterangan AS namapemeliharaanlain,
 pemeliharaanlain AS pemeliharaan,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 bypemeliharaan_lain;