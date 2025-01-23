SELECT
 @idpdam,
 id AS idsumberair,
 kodesumberair,
 sumberair AS namasumberair,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 sumberair;