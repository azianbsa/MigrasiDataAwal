SELECT
 @idpdam,
 na.id AS idnonair,
 jns.id AS idjenisnonair,
 pel.id AS idpelangganair,
 NULL AS idpelangganlimbah,
 NULL AS idpelangganlltt,
 IF(na.periode='',NULL,na.periode) AS kodeperiode,
 na.nomor AS nomornonair,
 na.keterangan AS keterangan,
 na.total AS total,
 na.tglmulaitagih AS tanggalmulaitagih,
 NULL AS tanggalkadaluarsa,
 na.nama AS nama,
 na.alamat AS alamat,
 ryn.id AS idrayon,
 NULL AS idkelurahan,
 gol.id AS idgolongan,
 NULL AS idtariflimbah,
 NULL AS idtariflltt,
 na.flagangsur AS flagangsur,
 NULL AS idangsuran,
 na.termin AS termin,
 0 AS flagmanual,
 NULL AS idpermohonansambunganbaru,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 nonair na
 LEFT JOIN pelanggan pel ON pel.nosamb = na.dibebankankepada
 LEFT JOIN temp_dataawal_jenisnonair jns ON jns.jenis = na.jenis
 LEFT JOIN rayon ryn ON ryn.koderayon = na.koderayon
 LEFT JOIN golongan gol ON gol.kodegol = na.kodegol AND gol.aktif = 1
 WHERE na.flagangsur = 1 AND na.flaghapus = 1 AND na.termin = 0 AND na.ketjenis NOT LIKE 'Uang_Muka%'