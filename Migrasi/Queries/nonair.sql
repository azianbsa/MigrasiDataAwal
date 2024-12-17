SELECT
 @idpdam,
 na.id AS idnonair,
 jns.id AS idjenisnonair,
 pel.id AS idpelangganair,
 NULL AS idpelangganlimbah,
 NULL AS idpelangganlltt,
 na.periode AS kodeperiode,
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
 LEFT JOIN [bsbs].pelanggan pel ON pel.nosamb = na.dibebankankepada
 LEFT JOIN jenisnonair jns ON jns.jenis = na.jenis
 LEFT JOIN [bsbs].rayon ryn ON ryn.koderayon = na.koderayon
 LEFT JOIN [bsbs].golongan gol ON gol.kodegol = na.kodegol AND gol.aktif = 1
 WHERE na.flaghapus = 0 AND na.flagangsur = 0 AND na.periode = @periode;