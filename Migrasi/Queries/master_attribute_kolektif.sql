SELECT
 @idpdam,
 id AS idkolektif,
 kodekolektif,
 kolektif AS namakolektif,
 ket AS keterangan,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 kolektif;