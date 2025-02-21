SELECT
 @idpdam,
 id AS idwilayah,
 kodewil AS kodewilayah,
 wilayah AS namawilayah,
 0 AS flagpusat,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 wilayah;