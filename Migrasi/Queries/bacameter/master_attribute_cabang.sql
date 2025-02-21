SELECT
 @idpdam,
 id AS idcabang,
 kodecabang,
 cabang AS namacabang,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 cabang;