SELECT
 @idpdam,
 k.id AS idkecamatan,
 k.kodekecamatan,
 k.kecamatan AS namakecamatan,
 c.id AS idcabang,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 kecamatan k
 JOIN cabang c ON c.kodecabang = k.kodecabang;